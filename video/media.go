package video

import (
	"sort"
	"sync"
)

/* The struct definitions here aims to represent the transcoded stream(s)
   and it's segment data in a table of nested maps as follows.
    ____________________________________________
   | Rendition |      | Segment # |      | Data |
   |___________|______|___________|______|______|
       360p0     --->     0.ts   --->      [...]
                          1.ts   --->      [...]

       1080p0    --->     0.ts   --->      [...]
                          1.ts   --->      [...]

   The inner map is accessed via TSegmentList representing the
   segments returned by the T for a given rendition (e.g. 360p0).
   It maps the segment index to the byte stream.

   The outer map is accessed via TRenditionList representing the
   renditions returned by the T. It maps the rendition name to the
   list of segments referenced by the inner map above.

   Since parallel jobs are used to transcode, all r/w accesses to
   these structs are protected to allow for atomic ops.
*/

type TSegmentList struct {
	mu               sync.Mutex
	SegmentDataTable map[int][]byte
}

func (s *TSegmentList) AddSegmentData(segIdx int, data []byte) {
	s.mu.Lock()
	s.SegmentDataTable[segIdx] = data
	s.mu.Unlock()
}

func (s *TSegmentList) GetSegment(segIdx int) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.SegmentDataTable[segIdx]
}

func (s *TSegmentList) GetSortedSegments() []int {
	segmentsTable := s.SegmentDataTable
	segments := make([]int, 0, len(segmentsTable))
	for k := range segmentsTable {
		segments = append(segments, k)
	}
	sort.Ints(segments)
	return segments
}

type TRenditionList struct {
	mu                    sync.Mutex
	RenditionSegmentTable map[string]*TSegmentList
}

func (r *TRenditionList) AddRenditionSegment(rendName string, sList *TSegmentList) {
	r.mu.Lock()
	r.RenditionSegmentTable[rendName] = sList
	r.mu.Unlock()
}

func (r *TRenditionList) GetSegmentList(rendName string) *TSegmentList {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.RenditionSegmentTable[rendName]
}

type RenditionStats struct {
	Name             string
	Width            int64
	Height           int64
	FPS              int64
	Bytes            int64
	DurationMs       float64
	ManifestLocation string
	BitsPerSecond    uint32
}
