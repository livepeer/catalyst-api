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
const waitForPushErrorIncreased = 2 * time.Minute
const keepStreamAfterEnd = 15 * time.Second
const statsCollectionPeriod = 10 * time.Second

const ownExchangeName = "lp_mist_api_connector"
const webhooksExchangeName = "webhook_default_exchange"
const eventMultistreamConnected = "multistream.connected"
const eventMultistreamError = "multistream.error"
const eventMultistreamDisconnected = "multistream.disconnected"
const eventMultistreamErrorTolerance = 2

type (
	// IMac creates new Mist API Connector application
	IMac interface {
		Start(ctx context.Context) error
		MetricsHandler() http.Handler
		RefreshMultistreamIfNeeded(playbackID string)
	}

	pushStatus struct {
		target              *api.MultistreamTarget
		profile             string
		lastEvent           string
		lastEventAt         time.Time
		lastEventErrorCount int
		metrics             *data.MultistreamMetrics
		mu                  sync.Mutex
	}

	streamInfo struct {
		id        string
		isLazy    bool
		stream    *api.Stream
		startedAt time.Time

		mu               sync.Mutex
		done             chan struct{}
		stopped          bool
		pushStatus       map[string]*pushStatus
		lastSeenBumpedAt time.Time
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
		mc.refreshMultistream(playbackID)
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
	waitForPushEvent := waitForPushError
	var waitForPushEventWasIncreased bool
	pushInfo.mu.Lock()
	if pushInfo.lastEventErrorCount > eventMultistreamErrorTolerance {
		// Most probably there is an issue with the multistream target; increase the wait time between PUSH_OUT_START
		// and PUSH_END to avoid flickering between 'connected' and 'error' events
		waitForPushEvent = waitForPushErrorIncreased
		waitForPushEventWasIncreased = true
	}
	pushInfo.mu.Unlock()

	select {
	case <-info.done:
		return
	case <-time.After(waitForPushEvent):
		info.mu.Lock()
		defer info.mu.Unlock()
		if info.stopped {
			return
		}
		pushInfo.mu.Lock()
		defer pushInfo.mu.Unlock()
                 pushDisabled := pushInfo.target.Disabled // push disabled in the meantime
                 eventAlreadySent := pushInfo.lastEvent == eventMultistreamConnected // no need to send it again
                 staleWait := time.Now().Add(-waitForPushEvent).Before(pushInfo.lastEventAt) // there was another event after PUSH_OUT, no need to send connected event
		if pushDisabled || eventAlreadySent || staleWait {
			return
		}
		pushInfo.lastEvent = eventMultistreamConnected
		pushInfo.lastEventAt = time.Now()
		if waitForPushEventWasIncreased {
			// Reset error count, because most probably the multistream target started to work
			pushInfo.lastEventErrorCount = 0
		}
		mc.emitWebhookEventAsync(info.stream, pushInfo, eventMultistreamConnected)
	}
}

func (mc *mac) emitMultistreamDisconnectedEvent(stream *streamInfo, status *pushStatus) {
	status.mu.Lock()
	defer status.mu.Unlock()
	status.lastEvent = eventMultistreamDisconnected
	status.lastEventAt = time.Now()
	mc.emitWebhookEventAsync(stream.stream, status, eventMultistreamDisconnected)

}

func (mc *mac) emitStreamStateEvent(stream *api.Stream, state data.StreamState) {
	streamID := stream.ParentID
	if streamID == "" {
		streamID = stream.ID
	}
	stateEvt := data.NewStreamStateEvent(mc.nodeID, mc.ownRegion, stream.UserID, streamID, state)
	mc.emitAmqpEvent(ownExchangeName, "stream.state."+streamID, stateEvt)
}

func (mc *mac) emitWebhookEventAsync(stream *api.Stream, pushInfo *pushStatus, eventKey string) {
	go func() {
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
	}()
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
			if info.stopped {
				// stream stopped, no need to send any events
				return
			}
			if pushInfo, ok := info.pushStatus[payload.Destination]; ok {
				pushInfo.mu.Lock()
				defer pushInfo.mu.Unlock()
				pushInfo.lastEventAt = time.Now()
				switch pushInfo.lastEvent {
				case eventMultistreamDisconnected:
					// push was disconnected, reset the lastEvent state
					pushInfo.lastEvent = ""
					pushInfo.lastEventErrorCount = 0
				default:
					pushInfo.lastEvent = eventMultistreamError
					if pushInfo.lastEventErrorCount <= eventMultistreamErrorTolerance {
						pushInfo.lastEventErrorCount++
					}
					if pushInfo.lastEventErrorCount == eventMultistreamErrorTolerance {
						// Trigger error event only after a few errors
						mc.emitWebhookEventAsync(info.stream, pushInfo, eventMultistreamError)
					}
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

// reconcileMultistreamLoop calls reconcileMultistream periodically or when multistreamUpdated is triggered on demand.
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

// reconcileMultistream makes sure that Mist contains the multistream pushes exactly as specified in streamInfo cache.
// There may be multiple reasons why Mist is not in sync with streamInfo cache:
// - streamInfo cache has changed (multistream target was turned on/off or multistream target was added/removed)
// - Mist removed its push for some reason
// Note that we use Mist AUTO_PUSH (which in turn makes sure that the PUSH is always available).
// Note also that we only create AUTO_PUSH for active streams which are ingest (not playback).
func (mc *mac) reconcileMultistream() {
	type key struct {
		stream string
		target string
	}
	toKey := func(stream, target string) key {
		return key{stream: stream, target: target}
	}
	isMultistream := func(k key) bool {
		return (strings.HasPrefix(k.stream, "video+") || strings.HasPrefix(k.stream, "videorec+")) &&
			(strings.HasPrefix(strings.ToLower(k.target), "rtmp:") || strings.HasPrefix(strings.ToLower(k.target), "srt:"))
	}

	mistState, err := mc.mist.GetState()
	if err != nil {
		glog.Errorf("error executing query on Mist, cannot reconcile multistream err=%v", err)
		return
	}

	// Get the existing PUSH_AUTO from Mist
	var filteredMistPushAutoList []*clients.MistPushAuto
	mistMap := map[key]bool{}
	for _, e := range mistState.PushAutoList {
		k := toKey(e.Stream, e.Target)
		if isMultistream(k) {
			filteredMistPushAutoList = append(filteredMistPushAutoList, e)
			mistMap[toKey(e.Stream, e.Target)] = true
		}
	}

	// Get the existing PUSH from Mist
	var filteredMistPushList []*clients.MistPush
	for _, e := range mistState.PushList {
		k := toKey(e.Stream, e.OriginalURL)
		if isMultistream(k) {
			filteredMistPushList = append(filteredMistPushList, e)
		}
	}

	// Get the expected multistreams from cached streamInfo
	type pushInfo struct {
		status  *pushStatus
		stream  *streamInfo
		enabled bool
	}
	cachedMap := map[key]*pushInfo{}
	mc.mu.Lock()
	for _, si := range mc.streamInfo {
		for target, v := range si.pushStatus {
			if v.target != nil {
				stream := mc.wildcardPlaybackID(si.stream)
				if isIngestStream(stream, si, mistState) {
					cachedMap[toKey(stream, target)] = &pushInfo{status: v, stream: si, enabled: !v.target.Disabled}
				}
			}
		}
	}
	mc.mu.Unlock()

	// Remove AUTO_PUSH that exists in Mist, but is not in streamInfo cache
	for _, e := range filteredMistPushAutoList {
		pi, exist := cachedMap[toKey(e.Stream, e.Target)]
		if !exist || !pi.enabled {
			if pi != nil {
				mc.emitMultistreamDisconnectedEvent(pi.stream, pi.status)
			}
			glog.Infof("removing AUTO_PUSH for stream=%s target=%s", e.Stream, e.Target)
			if err := mc.mist.PushAutoRemove(e.StreamParams); err != nil {
				glog.Errorf("cannot remove AUTO_PUSH for stream=%s target=%s err=%v", e.Stream, e.Target, err)
			}
		}
	}

	// Remove PUSH that exists in Mist, but is not in streamInfo cache
	// Deleted AUTO_PUSH does not automatically delete the related PUSH
	for _, e := range filteredMistPushList {
		pi, exist := cachedMap[toKey(e.Stream, e.OriginalURL)]
		if !exist || !pi.enabled {
			glog.Infof("stopping PUSH for stream=%s target=%s id=%d", e.Stream, e.OriginalURL, e.ID)
			if err := mc.mist.PushStop(e.ID); err != nil {
				glog.Errorf("cannot stop PUSH for stream=%s target=%s id=%d err=%v", e.Stream, e.OriginalURL, e.ID, err)
			}
		}
	}

	// Add AUTO_PUSH that exists streamInfo cache, but not in Mist
	for k, v := range cachedMap {
		if v.enabled && !mistMap[toKey(k.stream, k.target)] {
			glog.Infof("adding AUTO_PUSH for stream=%s target=%s", k.stream, k.target)
			if err := mc.mist.PushAutoAdd(k.stream, k.target); err != nil {
				glog.Errorf("cannot add AUTO_PUSH for stream=%s target=%s err=%v", k.stream, k.target, err)
			}
		}
	}
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

	stream, err := mc.lapi.GetStreamByPlaybackID(playbackID)
	if err != nil {
		return nil, fmt.Errorf("error getting stream by playback ID %s: %w", playbackID, err)
	}

	newPushes := make(map[string]*pushStatus)
	for _, ref := range stream.Multistream.Targets {
		target, pushURL, err := mc.getPushUrl(stream, &ref)
		if err != nil {
			glog.Errorf("error fetching multistream target, err=%v", err)
			continue
		}
		newPushes[pushURL] = &pushStatus{
			target:  target,
			profile: ref.Profile,
			metrics: &data.MultistreamMetrics{},
		}
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()
	info, exists := mc.streamInfo[playbackID]
	if !exists {
		info = &streamInfo{
			id:         stream.ID,
			stream:     stream,
			isLazy:     true, // flag it as a lazy stream info to avoid sending metrics
			done:       make(chan struct{}),
			pushStatus: newPushes,
		}
		mc.streamInfo[playbackID] = info
	} else {
		info.id = stream.ID
		info.stream = stream
	}
	info.mu.Lock()
	defer info.mu.Unlock()

	for newPushURL, newPush := range newPushes {
		if push, exists := info.pushStatus[newPushURL]; exists {
			push.mu.Lock()
			push.target = newPush.target
			push.profile = newPush.profile
			push.mu.Unlock()
		} else {
			info.pushStatus[newPushURL] = newPush
		}
	}
	for oldPushURL := range info.pushStatus {
		if _, exists := newPushes[oldPushURL]; !exists {
			delete(info.pushStatus, oldPushURL)
		}
	}

	glog.Infof("Refreshed stream info for playbackID=%s id=%s numPushes=%d", playbackID, stream.ID, len(info.pushStatus))

	return info, nil
}

func mistStreamName2playbackID(msn string) string {
	if strings.Contains(msn, "+") {
		return strings.Split(msn, "+")[1]
	}
	return msn
}

func pushToMultistreamTargetInfo(pushInfo *pushStatus) data.MultistreamTargetInfo {
	pushInfo.mu.Lock()
	defer pushInfo.mu.Unlock()
	return data.MultistreamTargetInfo{
		ID:      pushInfo.target.ID,
		Name:    pushInfo.target.Name,
		Profile: pushInfo.profile,
	}
}

// isIngestStream checks if the given stream is ingest (push) as opposed to the playback (pull) stream.
// We do need to check it in both streamInfo and MistState, because:
//   - streamInfo: isLazy is set to false in the PUSH_REWRITE trigger, which is present only for ingest streams; checking
//     this condition only is not good enough, because catalyst-api might be restarted and have isLazy set to true for
//     the ingest stream
//   - MistState: active_streams have source "push://" for ingest streams; checking this condition only is not good
//     enough, because a freshly started stream may not be yet visible in Mist (though it's already started).
func isIngestStream(stream string, si *streamInfo, mistState clients.MistState) bool {
	return !si.isLazy || mistState.IsIngestStream(stream)
}
