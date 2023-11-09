package video

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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
		// unfortunately I had to hard-code these, ffmpeg throws an error with our ts input otherwise
		// similar to https://stackoverflow.com/questions/48005093/ffmpeg-incompatible-with-output-codec-id
		// there doesn't seem to be a way for ffmpeg to work out the tags automatically,
		// if our codecs change we'll need to update these
		"-vtag", "avc1",
		"-atag", "mp4a",
	)
	args = append(args, mapArgs...)
	args = append(args, fmp4ManifestOutputFile)

	timeout, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
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

func ConcatTS(tsFileName string, segmentsList *TSegmentList, useStreamBasedConcat bool) (int64, error) {
	var totalBytes int64
	if !useStreamBasedConcat {
		// Create a .ts file for a given rendition
		tsFile, err := os.Create(tsFileName)
		if err != nil {
			return totalBytes, fmt.Errorf("error creating file (%s) err: %w", tsFileName, err)
		}
		defer tsFile.Close()
		// For a given rendition, write all segment indices in ascending order to the single .ts file
		for _, k := range segmentsList.GetSortedSegments() {
			segBytes, err := tsFile.Write(segmentsList.SegmentDataTable[k])
			if err != nil {
				return totalBytes, fmt.Errorf("error writing segment %d err: %w", k, err)
			}
			totalBytes = totalBytes + int64(segBytes)
		}
		return totalBytes, nil
	} else {
		// Strip the '.ts' from filename
		fileBaseWithoutExt := tsFileName[:len(tsFileName)-len(".ts")]

		// Create a text file containing filenames of the segments
		segmentListTxtFileName := fileBaseWithoutExt + ".txt"
		segmentListTxtFile, err := os.Create(segmentListTxtFileName)
		if err != nil {
			return totalBytes, fmt.Errorf("error creating  segment text file (%s) err: %w", segmentListTxtFileName, err)
		}
		defer segmentListTxtFile.Close()
		defer os.Remove(segmentListTxtFileName)
		w := bufio.NewWriter(segmentListTxtFile)

		// Write each segment to disk and add segment filename to the text file
		for segName, segData := range segmentsList.GetSortedSegments() {
			// Open a new file to write each segment to disk
			segmentFileName := fileBaseWithoutExt + "_" + strconv.Itoa(segName) + ".ts"
			segmentFile, err := os.Create(segmentFileName)
			if err != nil {
				return totalBytes, fmt.Errorf("error creating individual segment file (%s) err: %w", segmentFileName, err)
			}
			defer segmentFile.Close()
			//defer os.Remove(segmentFileName)
			// Write the segment data to disk
			segBytes, err := segmentFile.Write(segmentsList.SegmentDataTable[segData])
			if err != nil {
				return totalBytes, fmt.Errorf("error writing segment %d err: %w", segName, err)
			}
			totalBytes = totalBytes + int64(segBytes)
			// Add filename to the text file
			line := fmt.Sprintf("file '%s'\n", segmentFileName)
			if _, err = w.WriteString(line); err != nil {
				return totalBytes, fmt.Errorf("error writing segment %d to text file err: %w", segName, err)
			}
			// Flush to make sure all buffered operations are applied
			if err = w.Flush(); err != nil {
				return totalBytes, fmt.Errorf("error flushing text file %s err: %w", segmentFileName, err)
			}
		}
		// Create a .ts file for a given rendition
		tsFile, err := os.Create(tsFileName)
		if err != nil {
			return totalBytes, fmt.Errorf("error creating file (%s) err: %w", tsFileName, err)
		}
		defer tsFile.Close()
		// Transmux the individual .ts files into a combined single ts file using stream based concatenation
		err = ffmpeg.Input(segmentListTxtFileName, ffmpeg.KwArgs{
			"f":    "concat", // Use stream based concatenation (instead of file based concatenation)
			"safe": "0"}).    // Must be 0 since relative paths to segments are used in segmentListTxtFileName
			Output(tsFileName, ffmpeg.KwArgs{
				"c": "copy", // Don't accidentally transcode
			}).
			OverWriteOutput().ErrorToStdOut().Run()
		if err != nil {
			return totalBytes, fmt.Errorf("failed to transmux multiple ts files from %s into a ts file: %w", segmentListTxtFileName, err)
		}
		// Verify the ts output file was created
		_, err = os.Stat(tsFileName)
		if err != nil {
			return totalBytes, fmt.Errorf("transmux error: failed to stat .ts media file: %w", err)
		}

		return totalBytes, nil
	}

}
