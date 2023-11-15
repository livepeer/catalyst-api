package middleware

import (
	"fmt"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
)

type SystemInfo struct {
	CPUInfo  []cpu.InfoStat
	MemInfo  *mem.VirtualMemoryStat
	DiskInfo []disk.UsageStat
	LoadInfo *load.AvgStat
}

/*
func main() {
    sysInfo, err := GetSystemInfo()
    if err != nil {
        fmt.Println("Error:", err)
        return
    }
    for _, cpu := range sysInfo.CPUInfo {
    	fmt.Println("CPU Info:\n", cpu.String())
    }
    fmt.Println("Mem Info:\n", sysInfo.MemInfo.String())
    fmt.Println("Load Info:\n", sysInfo.LoadInfo.String())
}
*/

// GetSystemInfo gathers the system's CPU, memory, and disk information
func GetSystemInfo() (*SystemInfo, error) {
	sysInfo := &SystemInfo{}

	// Get CPU information
	cpuInfo, err := cpu.Info()
	if err != nil {
		return nil, err
	}
	sysInfo.CPUInfo = cpuInfo

	// Get memory information
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		return nil, err
	}
	sysInfo.MemInfo = memInfo

	// Get disk information
	partitions, err := disk.Partitions(true)
	if err != nil {
		return nil, err
	}

	for _, p := range partitions {
		diskInfo, err := disk.Usage(p.Mountpoint)
		if err != nil {
			return nil, err
		}
		sysInfo.DiskInfo = append(sysInfo.DiskInfo, *diskInfo)
	}

	// Get load info
	loadInfo, err := load.Avg()
	if err != nil {
		return nil, err
	}
	sysInfo.LoadInfo = loadInfo

	return sysInfo, nil
}
