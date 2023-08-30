package video

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	ffmpeg "github.com/u2takey/ffmpeg-go"
)

func MuxTStoMP4(tsInputFile, mp4OutputFile string) ([]string, error) {
	var transmuxOutputFiles []string
	// transmux the .ts file into a standalone MP4 file
	err := ffmpeg.Input(tsInputFile).
		Output(mp4OutputFile, ffmpeg.KwArgs{
			"analyzeduration": "15M",           // Analyze up to 15s of video to figure out the format. We saw failures to detect the video codec without this
			"movflags":        "faststart",     // Need this for progressive playback and probing
			"c":               "copy",          // Don't accidentally transcode
			"bsf:a":           "aac_adtstoasc", // Remove ADTS header (required for ts -> mp4 container conversion)
		}).
		OverWriteOutput().ErrorToStdOut().Run()
	if err != nil {
		return nil, fmt.Errorf("failed to transmux concatenated mpeg-ts file (%s) into a mp4 file: %w", tsInputFile, err)
	}
	// Verify the mp4 output file was created
	_, err = os.Stat(mp4OutputFile)
	if err != nil {
		return nil, fmt.Errorf("transmux error: failed to stat MP4 media file: %w", err)
	} else {
		transmuxOutputFiles = append(transmuxOutputFiles, mp4OutputFile)
	}
	return transmuxOutputFiles, nil
}

func MuxTStoFMP4(fmp4ManifestOutputFile string, inputs ...string) error {
	baseFragMp4Dir := filepath.Dir(fmp4ManifestOutputFile)
	err := os.Mkdir(baseFragMp4Dir, 0700)
	if err != nil && !os.IsExist(err) {
		return fmt.Errorf("transmux error: failed to create subdir to output fmp4 files: %w", err)
	}

	var args []string
	mapArgs := []string{"-map", "0:a"}
	for i, input := range inputs {
		args = append(args, "-i", input)
		mapArgs = append(mapArgs, "-map", fmt.Sprintf("%d:v", i))
	}
	args = append(args,
		"-movflags", "frag_keyframe+empty_moov",
		"-c", "copy",
		"-bsf:a", "aac_adtstoasc",
		"-f", "dash",
		"-dash_segment_type", "mp4",
		"-single_file", "1",
		"-hls_playlist", "1",
		"-hls_time", "10",
		"-hls_playlist_type", "vod",
		"-hls_segment_type", "fmp4",
		"-vtag", "avc1",
		"-atag", "mp4a",
	)
	args = append(args, mapArgs...)
	args = append(args, fmp4ManifestOutputFile)

	timeout, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(timeout, "ffmpeg", args...)

	var outputBuf bytes.Buffer
	var stdErr bytes.Buffer
	cmd.Stdout = &outputBuf
	cmd.Stderr = &stdErr

	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("error running ffmpeg [%s] [%s] %w", outputBuf.String(), stdErr.String(), err)
	}

	_, err = os.Stat(fmp4ManifestOutputFile)
	if err != nil {
		return fmt.Errorf("transmux error: failed to stat FMP4 Dash manifest [%s] [%s]: %w", outputBuf.String(), stdErr.String(), err)
	}
	_, err = os.Stat(filepath.Join(baseFragMp4Dir, "master.m3u8"))
	if err != nil {
		return fmt.Errorf("transmux error: failed to stat FMP4 HLS manifest [%s] [%s]: %w", outputBuf.String(), stdErr.String(), err)
	}
	return nil
}

func ConcatTS(tsFileName string, segmentsList *TSegmentList) (int64, error) {
	var totalBytes int64
	// 1. create a .ts file for a given rendition
	tsFile, err := os.Create(tsFileName)
	if err != nil {
		return totalBytes, fmt.Errorf("error creating file (%s) err: %w", tsFileName, err)
	}
	defer tsFile.Close()
	// 2. for a given rendition, write all segment indices in ascending order to the single .ts file
	for _, k := range segmentsList.GetSortedSegments() {
		segBytes, err := tsFile.Write(segmentsList.SegmentDataTable[k])
		if err != nil {
			return totalBytes, fmt.Errorf("error writing segment %d err: %w", k, err)
		}
		totalBytes = totalBytes + int64(segBytes)
	}
	return totalBytes, nil
}
