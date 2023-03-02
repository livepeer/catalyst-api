package video

import (
	"fmt"
	ffmpeg "github.com/u2takey/ffmpeg-go"
	"os"
)

func MuxTStoMP4(tsInputFile, mp4OutputFile string) error {
	// transmux the .ts file into mp4
	err := ffmpeg.Input(tsInputFile).
		Output(mp4OutputFile, ffmpeg.KwArgs{"movflags": "faststart", "c": "copy", "bsf:a": "aac_adtstoasc"}).
		OverWriteOutput().ErrorToStdOut().Run()
	if err != nil {
		return fmt.Errorf("failed to transmux concatenated mpeg-ts file (%s) into a mp4 file: %s", tsInputFile, err)
	}
	return nil
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
