package video

import (
	"fmt"
	ffmpeg "github.com/u2takey/ffmpeg-go"
	"os"
	"path/filepath"
)

func MuxTStoMP4(tsInputFile, mp4OutputFile string) ([]string, error) {
	var transmuxOutputFiles []string
	// transmux the .ts file into a standalone MP4 file
	err := ffmpeg.Input(tsInputFile).
		Output(mp4OutputFile, ffmpeg.KwArgs{"movflags": "faststart", "c": "copy", "bsf:a": "aac_adtstoasc"}).
		OverWriteOutput().ErrorToStdOut().Run()
	if err != nil {
		return nil, fmt.Errorf("failed to transmux concatenated mpeg-ts file (%s) into a mp4 file: %s", tsInputFile, err)
	}
	// Verify the mp4 output file was created
	_, err = os.Stat(mp4OutputFile)
	if err != nil {
		return nil, fmt.Errorf("transmux error: failed to stat MP4 media file: %s", err)
	} else {
		transmuxOutputFiles = append(transmuxOutputFiles, mp4OutputFile)
	}
	return transmuxOutputFiles, nil
}

func MuxTStoFMP4WithHLS(tsInputFile, fmp4ManifestOutputFile string) ([]string, error) {

	baseFragMp4Dir := filepath.Dir(fmp4ManifestOutputFile)
	err := os.Mkdir(baseFragMp4Dir, 0700)
	if err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("transmux error: failed to create subdir to output fmp4 files: %s", err)
	}

	var transmuxOutputFiles []string
	// transmux the .ts file into fMP4 packaged with HLS
	err = ffmpeg.Input(tsInputFile).
		Output(fmp4ManifestOutputFile, ffmpeg.KwArgs{"movflags": "frag_keyframe+empty_moov",
			"c":                 "copy",
			"bsf:a":             "aac_adtstoasc",
			"hls_time":          10,
			"hls_playlist_type": "vod",
			"hls_segment_type":  "fmp4",
			"hls_flags":         "single_file"}).
		OverWriteOutput().ErrorToStdOut().Run()
	if err != nil {
		return nil, fmt.Errorf("transmux error: failed to transmux concatenated mpeg-ts file (%s) into a fragemented-mp4 file: %s", tsInputFile, err)
	}

	fmp4VideoOutputFile := fmp4ManifestOutputFile[:len(fmp4ManifestOutputFile)-len(filepath.Ext(fmp4ManifestOutputFile))] + ".m4s"

	// Verify the fmp4 manifest output file was created
	_, err = os.Stat(fmp4ManifestOutputFile)
	if err != nil {
		return nil, fmt.Errorf("transmux error: failed to stat fMP4 manifest file: %s", err)
	} else {
		transmuxOutputFiles = append(transmuxOutputFiles, fmp4ManifestOutputFile)
	}

	// Verify the fmp4 media output file was created
	_, err = os.Stat(fmp4VideoOutputFile)
	if err != nil {
		return nil, fmt.Errorf("transmux error: failed to stat fMP4 media file: %s", err)
	} else {
		transmuxOutputFiles = append(transmuxOutputFiles, fmp4VideoOutputFile)
	}

	return transmuxOutputFiles, nil
}

func ConcatTS(tsFileName string, segmentsList *TSegmentList) (int64, error) {
	var totalBytes int64
	// 1. create a .ts file for a given rendition
	tsFile, err := os.Create(tsFileName)
	if err != nil {
		return totalBytes, fmt.Errorf("error creating file (%s) err: %s", tsFileName, err)
	}
	defer tsFile.Close()
	// 2. for a given rendition, write all segment indices in ascending order to the single .ts file
	for _, k := range segmentsList.GetSortedSegments() {
		segBytes, err := tsFile.Write(segmentsList.SegmentDataTable[k])
		if err != nil {
			return totalBytes, fmt.Errorf("error writing segment %d err: %s", k, err)
		}
		totalBytes = totalBytes + int64(segBytes)
	}
	return totalBytes, nil
}
