//nolint:all
package mistapiconnector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/handlers/misttriggers"
	"github.com/livepeer/catalyst-api/mapic/metrics"
	"github.com/livepeer/catalyst-api/mapic/model"
	"github.com/livepeer/go-api-client"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
)

const streamPlaybackPrefix = "playback_"
const audioAlways = "always"
const audioRecord = "record"
const audioEnabledStreamSuffix = "rec"
const waitForPushError = 7 * time.Second
const keepStreamAfterEnd = 15 * time.Second
const statsCollectionPeriod = 10 * time.Second

const ownExchangeName = "lp_mist_api_connector"
const webhooksExchangeName = "webhook_default_exchange"
const eventMultistreamConnected = "multistream.connected"
const eventMultistreamError = "multistream.error"
const eventMultistreamDisconnected = "multistream.disconnected"

type (
	// IMac creates new Mist API Connector application
	IMac interface {
		Start(ctx context.Context) error
		MetricsHandler() http.Handler
		RefreshMultistreamIfNeeded(playbackID string)
	}

	pushStatus struct {
		target           *api.MultistreamTarget
		profile          string
		pushStartEmitted bool
		pushStopped      bool
		metrics          *data.MultistreamMetrics
	}

	streamInfo struct {
		id        string
		isLazy    bool
		stream    *api.Stream
		startedAt time.Time

		mu                 sync.Mutex
		done               chan struct{}
		stopped            bool
		multistreamStarted bool
		pushStatus         map[string]*pushStatus
		lastSeenBumpedAt   time.Time
	}

	// MacOptions configuration object
	MacOptions struct {
		NodeID, MistHost string
		LivepeerAPI      *api.Client
		BalancerHost     string
		CheckBandwidth   bool
		RoutePrefix, PlaybackDomain, MistURL,
		SendAudio, BaseStreamName string
		AMQPUrl, OwnRegion        string
		MistStreamSource          string
		MistHardcodedBroadcasters string
		NoMistScrapeMetrics       bool
	}

	mac struct {
		ctx                       context.Context
		cancel                    context.CancelFunc
		lapi                      *api.Client
		balancerHost              string
		mu                        sync.RWMutex
		mistHot                   string
		checkBandwidth            bool
		baseStreamName            string
		streamInfo                map[string]*streamInfo
		producer                  event.AMQPProducer
		nodeID                    string
		ownRegion                 string
		mistStreamSource          string
		mistHardcodedBroadcasters string
		config                    *config.Cli
		broker                    misttriggers.TriggerBroker
		mist                      clients.MistAPIClient
		multistreamUpdated        chan struct{}
	}
)

func (mc *mac) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// todo: you're not supposed to store these...
	mc.ctx = ctx
	mc.cancel = cancel

	mc.broker.OnStreamBuffer(mc.handleStreamBuffer)
	mc.broker.OnPushRewrite(mc.handlePushRewrite)
	mc.broker.OnLiveTrackList(mc.handleLiveTrackList)
	mc.broker.OnPushOutStart(mc.handlePushOutStart)
	mc.broker.OnPushEnd(mc.handlePushEnd)

	lapi, _ := api.NewAPIClientGeolocated(api.ClientOptions{
		Server:      mc.config.APIServer,
		AccessToken: mc.config.APIToken,
	})
	mc.lapi = lapi

	if mc.balancerHost != "" && !strings.Contains(mc.balancerHost, ":") {
		mc.balancerHost = mc.balancerHost + ":8042" // must set default port for Mist's Load Balancer
	}
	var producer event.AMQPProducer
	if mc.config.AMQPURL != "" {
		pu, err := url.Parse(mc.config.AMQPURL)
		if err != nil {
			return fmt.Errorf("error parsing AMQP url err=%w", err)
		}

		glog.Infof("Creating AMQP producer with url=%s", pu.Redacted())
		setup := func(c event.AMQPChanSetup) error {
			if err := c.ExchangeDeclarePassive(webhooksExchangeName, "topic", true, false, false, false, nil); err != nil {
				glog.Warningf("mist-api-connector: Webhooks exchange does not exist. exchange=%s err=%v", webhooksExchangeName, err)
			}
			return c.ExchangeDeclare(ownExchangeName, "topic", true, false, false, false, nil)
		}
		producer, err = event.NewAMQPProducer(mc.config.AMQPURL, event.NewAMQPConnectFunc(setup))
		if err != nil {
			return err
		}
		mc.producer = producer
	} else {
		glog.Infof("AMQP url is empty!")
	}
	if producer != nil && mc.config.MistScrapeMetrics {
		startMetricsCollector(ctx, statsCollectionPeriod, mc.nodeID, mc.ownRegion, mc.mist, lapi, producer, ownExchangeName, mc)
	}

	mc.multistreamUpdated = make(chan struct{}, 1)
	go func() {
		mc.reconcileMultistreamLoop(ctx)
	}()

	<-ctx.Done()
	return nil
}

func (mc *mac) MetricsHandler() http.Handler {
	return metrics.Exporter
}

func (mc *mac) RefreshMultistreamIfNeeded(playbackID string) {
	if mc.streamExists(playbackID) {
		mc.refreshMultistream(playbackID)
	}
}

func (mc *mac) handleStreamBuffer(ctx context.Context, payload *misttriggers.StreamBufferPayload) error {
	// We only care about connections ending
	if !payload.IsEmpty() {
		return nil
	}

	playbackID := payload.StreamName
	if mc.baseStreamName != "" && strings.Contains(playbackID, "+") {
		playbackID = strings.Split(playbackID, "+")[1]
	}
	if info, ok := mc.getStreamInfoLogged(playbackID); ok {
		glog.Infof("Setting stream's manifestID=%s playbackID=%s active status to false", info.id, playbackID)
		_, err := mc.lapi.SetActive(info.id, false, info.startedAt)
		if err != nil {
			glog.Error(err)
		}
		mc.emitStreamStateEvent(info.stream, data.StreamState{Active: false})
		info.mu.Lock()
		info.stopped = true
		info.mu.Unlock()
		mc.removeInfoDelayed(playbackID, info.done)
		metrics.StopStream(true)
	}

	return nil
}

func (mc *mac) handlePushRewrite(ctx context.Context, payload *misttriggers.PushRewritePayload) (string, error) {
	streamKey := payload.StreamName
	var responseName string
	if payload.URL.Scheme == "rtmp" {
		pp := strings.Split(payload.URL.Path, "/")
		if len(pp) != 3 {
			glog.Errorf("Push rewrite URL wrongly formatted - should be in format rtmp://mist.host/live/streamKey payload=%s", payload)
			return "", nil
		}
	}
	glog.V(model.VVERBOSE).Infof("Requested stream key is '%s'", streamKey)
	// ask API
	stream, err := mc.lapi.GetStreamByKey(streamKey)
	if errors.Is(err, api.ErrNotExists) {
		glog.Errorf("Stream not found for push rewrite streamKey=%s err=%v", streamKey, err)
		return "", nil
	} else if err != nil || stream == nil {
		return "", fmt.Errorf("Error getting stream info from Livepeer API streamKey=%s err=%v", streamKey, err)
	}
	glog.V(model.VERBOSE).Infof("For stream %s got info %+v", streamKey, stream)

	if stream.PlaybackID != "" {
		mc.mu.Lock()
		if info, ok := mc.streamInfo[stream.PlaybackID]; ok {
			info.mu.Lock()
			glog.Infof("Stream playbackID=%s stopped=%v already in map, removing its info", stream.PlaybackID, info.stopped)
			info.mu.Unlock()
			mc.removeInfoLocked(stream.PlaybackID)
		}
		info := &streamInfo{
			id:         stream.ID,
			stream:     stream,
			done:       make(chan struct{}),
			pushStatus: make(map[string]*pushStatus),
			startedAt:  time.Now(),
		}
		mc.streamInfo[stream.PlaybackID] = info
		mc.mu.Unlock()
		streamKey = stream.PlaybackID
		// streamKey = strings.ReplaceAll(streamKey, "-", "")
		if mc.balancerHost != "" {
			streamKey = streamPlaybackPrefix + streamKey
		}
		if mc.baseStreamName == "" {
			responseName = streamKey
		} else {
			responseName = mc.wildcardPlaybackID(stream)
		}
		ok, err := mc.lapi.SetActive(stream.ID, true, info.startedAt)
		if err != nil {
			return "", fmt.Errorf("Error calling SetActive err=%s", err)
		} else if !ok {
			glog.Infof("Stream id=%s streamKey=%s playbackId=%s forbidden by webhook, rejecting", stream.ID, stream.StreamKey, stream.PlaybackID)
			mc.removeInfo(stream.PlaybackID)
			return "", nil
		}
	} else {
		glog.Errorf("Shouldn't happen streamID=%s", stream.ID)
	}
	go mc.emitStreamStateEvent(stream, data.StreamState{Active: true})
	metrics.StartStream()
	glog.Infof("Responded with '%s'", responseName)
	return responseName, nil
}

func (mc *mac) handleLiveTrackList(ctx context.Context, payload *misttriggers.LiveTrackListPayload) error {
	go func() {
		videoTracksNum := payload.CountVideoTracks()
		playbackID := mistStreamName2playbackID(payload.StreamName)
		glog.Infof("for video %s got %d video tracks", playbackID, videoTracksNum)

		if videoTracksNum > 1 {
			mc.refreshMultistream(playbackID)
		}
	}()
	return nil
}

func (mc *mac) streamExists(playbackID string) bool {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	_, streamExists := mc.streamInfo[playbackID]
	return streamExists
}

func (mc *mac) refreshMultistream(playbackID string) {
	_, err := mc.refreshStreamInfo(playbackID)
	if err != nil {
		glog.Errorf("Error refreshing stream info for playbackID=%s", playbackID)
		return
	}
	select {
	case mc.multistreamUpdated <- struct{}{}:
		// trigger reconcile multistream
	default:
		// do not block if reconcile multistream already triggered
	}
}

func (mc *mac) handlePushOutStart(ctx context.Context, payload *misttriggers.PushOutStartPayload) (string, error) {
	go func() {
		playbackID := mistStreamName2playbackID(payload.StreamName)
		if info, ok := mc.getStreamInfoLogged(playbackID); ok {
			info.mu.Lock()
			defer info.mu.Unlock()
			if pushInfo, ok := info.pushStatus[payload.URL]; ok {
				go mc.waitPush(info, pushInfo)
			} else {
				glog.Errorf("For stream playbackID=%s got unknown RTMP push %s", playbackID, payload.URL)
			}
		}

	}()
	return payload.URL, nil
}

// waits for RTMP push error
func (mc *mac) waitPush(info *streamInfo, pushInfo *pushStatus) {
	select {
	case <-info.done:
		return
	case <-time.After(waitForPushError):
		info.mu.Lock()
		defer info.mu.Unlock()
		if info.stopped {
			return
		}
		if !pushInfo.pushStopped {
			// there was no error starting RTMP push, so no we can send 'multistream.connected' webhook event
			pushInfo.pushStartEmitted = true
			mc.emitWebhookEvent(info.stream, pushInfo, eventMultistreamConnected)
		}
	}
}

func (mc *mac) emitStreamStateEvent(stream *api.Stream, state data.StreamState) {
	streamID := stream.ParentID
	if streamID == "" {
		streamID = stream.ID
	}
	stateEvt := data.NewStreamStateEvent(mc.nodeID, mc.ownRegion, stream.UserID, streamID, state)
	mc.emitAmqpEvent(ownExchangeName, "stream.state."+streamID, stateEvt)
}

func (mc *mac) emitWebhookEvent(stream *api.Stream, pushInfo *pushStatus, eventKey string) {
	streamID, sessionID := stream.ParentID, stream.ID
	if streamID == "" {
		streamID = sessionID
	}
	payload := data.MultistreamWebhookPayload{
		Target: pushToMultistreamTargetInfo(pushInfo),
	}
	hookEvt, err := data.NewWebhookEvent(streamID, eventKey, stream.UserID, sessionID, payload)
	if err != nil {
		glog.Errorf("Error creating webhook event err=%v", err)
		return
	}
	mc.emitAmqpEvent(webhooksExchangeName, "events."+eventKey, hookEvt)
}

func (mc *mac) emitAmqpEvent(exchange, key string, evt data.Event) {
	if mc.producer == nil {
		return
	}
	glog.Infof("Publishing amqp message to exchange=%s key=%s msg=%+v", exchange, key, evt)

	ctx, cancel := context.WithTimeout(mc.ctx, 3*time.Second)
	defer cancel()
	err := mc.producer.Publish(ctx, event.AMQPMessage{
		Exchange:   exchange,
		Key:        key,
		Body:       evt,
		Persistent: true,
	})
	if err != nil {
		glog.Errorf("Error publishing amqp message to exchange=%s key=%s, err=%v", exchange, key, err)
	}
}

func (mc *mac) handlePushEnd(ctx context.Context, payload *misttriggers.PushEndPayload) error {
	go func() {
		playbackID := mistStreamName2playbackID(payload.StreamName)
		if info, ok := mc.getStreamInfoLogged(playbackID); ok {
			info.mu.Lock()
			defer info.mu.Unlock()
			if pushInfo, ok := info.pushStatus[payload.Destination]; ok {
				if pushInfo.pushStartEmitted {
					// emit normal push.end
					mc.emitWebhookEvent(info.stream, pushInfo, eventMultistreamDisconnected)
				} else {
					pushInfo.pushStopped = true
					//  emit push error
					mc.emitWebhookEvent(info.stream, pushInfo, eventMultistreamError)
				}
			} else {
				glog.Errorf("For stream playbackID=%s got unknown RTMP push %s", playbackID, payload.StreamName)
			}
		}
	}()
	return nil
}

func (mc *mac) removeInfoDelayed(playbackID string, done chan struct{}) {
	go func() {
		select {
		case <-done:
			return
		case <-time.After(keepStreamAfterEnd):
			mc.removeInfo(playbackID)
		}
	}()
}

func (mc *mac) removeInfo(playbackID string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.removeInfoLocked(playbackID)
}

// must be called inside mc.mu.Lock
func (mc *mac) removeInfoLocked(playbackID string) {
	if info, ok := mc.streamInfo[playbackID]; ok {
		close(info.done)
		delete(mc.streamInfo, playbackID)
	}
}

func (mc *mac) wildcardPlaybackID(stream *api.Stream) string {
	return mc.baseNameForStream(stream) + "+" + stream.PlaybackID
}

func (mc *mac) baseNameForStream(stream *api.Stream) string {
	baseName := mc.baseStreamName
	if mc.shouldEnableAudio(stream) {
		baseName += audioEnabledStreamSuffix
	}
	return baseName
}

func (mc *mac) shouldEnableAudio(stream *api.Stream) bool {
	audio := false
	if mc.config.MistSendAudio == audioAlways {
		audio = true
	} else if mc.config.MistSendAudio == audioRecord {
		audio = stream.Record
	}
	return audio
}

func (mc *mac) reconcileMultistreamLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-mc.multistreamUpdated:
		}
		mc.reconcileMultistream()
	}
}

func (mc *mac) reconcileMultistream() {
	type key struct {
		stream string
		target string
	}
	toKey := func(stream, target string) key {
		return key{stream: stream, target: target}
	}

	// Get the existing PUSH_AUTO from Mist
	mistPushAutoList, err := mc.mist.PushAutoList()
	if err != nil {
		glog.Errorf("error executing PUSH_AUTO_LIST on Mist, cannot reconcile multistream err=%v", err)
		return
	}
	filteredMistPushAutoList := filterMultistream(mistPushAutoList)
	mistMap := map[key]bool{}
	for _, e := range filteredMistPushAutoList {
		mistMap[toKey(e.Stream, e.Target)] = true
	}

	// Get the expected multistreams from cached streamInfo
	cachedMap := map[key]bool{}
	mc.mu.Lock()
	for _, si := range mc.streamInfo {
		for target, v := range si.pushStatus {
			if v.target != nil && !v.target.Disabled {
				stream := mc.wildcardPlaybackID(si.stream)
				cachedMap[toKey(stream, target)] = true
			}
		}
	}
	mc.mu.Unlock()

	// Remove AUTO_PUSH that exists in Mist, but is not in streamInfo cache
	for _, e := range filteredMistPushAutoList {
		if !cachedMap[toKey(e.Stream, e.Target)] {
			glog.Infof("removing AUTO_PUSH for stream=%s target=%s", e.Stream, e.Target)
			if err := mc.mist.PushAutoRemove(e.StreamParams); err != nil {
				glog.Errorf("cannot remove AUTO_PUSH for stream=%s target=%s err=%v", e.Stream, e.Target, err)
			}
		}
	}

	// Add AUTO_PUSH that exists streamInfo cache, but not in Mist
	for k, _ := range cachedMap {
		if !mistMap[toKey(k.stream, k.target)] {
			glog.Infof("adding AUTO_PUSH for stream=%s target=%s", k.stream, k.target)
			if err := mc.mist.PushAutoAdd(k.stream, k.target); err != nil {
				glog.Errorf("cannot add AUTO_PUSH for stream=%s target=%s err=%v", k.stream, k.target, err)
			}
		}
	}
}

func filterMultistream(list []clients.MistPushAuto) []clients.MistPushAuto {
	var res []clients.MistPushAuto
	for _, e := range list {
		if (strings.HasPrefix(e.Stream, "video+") || strings.HasPrefix(e.Stream, "videorec+")) &&
			(strings.HasPrefix(strings.ToLower(e.Target), "rtmp:") || strings.HasPrefix(strings.ToLower(e.Target), "srt:")) {
			res = append(res, e)
		}
	}
	return res
}

func (mc *mac) getPushUrl(stream *api.Stream, targetRef *api.MultistreamTargetRef) (*api.MultistreamTarget, string, error) {
	target, err := mc.lapi.GetMultistreamTarget(targetRef.ID)
	if err != nil {
		return nil, "", fmt.Errorf("error fetching multistream target %s: %w", targetRef.ID, err)
	}
	// Find the actual parameters of the profile we're using
	var videoSelector string
	// Not actually the source. But the highest quality.
	if targetRef.Profile == "source" {
		videoSelector = "maxbps"
	} else {
		var prof *api.Profile
		for _, p := range stream.Profiles {
			if p.Name == targetRef.Profile {
				prof = &p
				break
			}
		}
		if prof == nil {
			return nil, "", fmt.Errorf("profile not found: %s", targetRef.Profile)
		}
		videoSelector = fmt.Sprintf("~%dx%d", prof.Width, prof.Height)
	}
	join := "?"
	if strings.Contains(target.URL, "?") {
		join = "&"
	}
	audioSelector := "maxbps"
	if targetRef.VideoOnly {
		audioSelector = "silent"
	}
	// Inject ?video=~widthxheight to send the correct rendition
	return target, fmt.Sprintf("%s%svideo=%s&audio=%s", target.URL, join, videoSelector, audioSelector), nil
}

func (mc *mac) getStreamInfoLogged(playbackID string) (*streamInfo, bool) {
	info, err := mc.getStreamInfo(playbackID)
	if err != nil {
		glog.Errorf("Error getting stream info playbackID=%q err=%q", playbackID, err)
		return nil, false
	}
	return info, true
}

func (mc *mac) getStreamInfo(playbackID string) (*streamInfo, error) {
	playbackID = mistStreamName2playbackID(playbackID)

	mc.mu.RLock()
	info := mc.streamInfo[playbackID]
	mc.mu.RUnlock()

	if info == nil {
		return mc.refreshStreamInfo(playbackID)
	}
	return info, nil
}

func (mc *mac) refreshStreamInfo(playbackID string) (*streamInfo, error) {
	glog.Infof("Refreshing stream info for playbackID=%s", playbackID)
	mc.mu.Lock()
	info, infoExists := mc.streamInfo[playbackID]
	mc.mu.Unlock()

	stream, err := mc.lapi.GetStreamByPlaybackID(playbackID)
	if err != nil {
		return nil, fmt.Errorf("error getting stream by playback ID %s: %w", playbackID, err)
	}

	newPushes := make(map[string]*pushStatus)
	for _, ref := range stream.Multistream.Targets {
		target, pushURL, err := mc.getPushUrl(stream, &ref)
		if err != nil {
			return nil, err
		}
		newPushes[pushURL] = &pushStatus{
			target:  target,
			profile: ref.Profile,
			// Assume setup was all successful
			pushStartEmitted: true,
			metrics:          &data.MultistreamMetrics{},
		}
		if infoExists {
			info.mu.Lock()
			push, pushExists := info.pushStatus[pushURL]
			info.mu.Unlock()
			if pushExists {
				newPushes[pushURL].metrics = push.metrics
				newPushes[pushURL].pushStopped = push.pushStopped
				newPushes[pushURL].pushStartEmitted = push.pushStartEmitted
			}
		}
	}

	newInfo := &streamInfo{
		id:         stream.ID,
		stream:     stream,
		isLazy:     true, // flag it as a lazy stream info to avoid sending metrics
		done:       make(chan struct{}),
		pushStatus: newPushes,
		// Assume setup was all successful
		multistreamStarted: true,
	}
	glog.Infof("Refreshed stream info for playbackID=%s id=%s numPushes=%d", playbackID, stream.ID, len(newPushes))

	mc.mu.Lock()
	mc.streamInfo[playbackID] = newInfo
	mc.mu.Unlock()

	return info, nil
}

func mistStreamName2playbackID(msn string) string {
	if strings.Contains(msn, "+") {
		return strings.Split(msn, "+")[1]
	}
	return msn
}

func pushToMultistreamTargetInfo(pushInfo *pushStatus) data.MultistreamTargetInfo {
	return data.MultistreamTargetInfo{
		ID:      pushInfo.target.ID,
		Name:    pushInfo.target.Name,
		Profile: pushInfo.profile,
	}
}
