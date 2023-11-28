package catalyst

import (
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
)

type SystemInfo struct {
	CPUUsagePercentage       float64
	RAMUsagePercentage       float64
	BandwidthUsagePercentage float64
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

	// Get memory information
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		return nil, err
	}
	sysInfo.RAMUsagePercentage = memInfo.UsedPercent

	// Get CPU information
	cpuInfo, err := cpu.Info()
	if err != nil {
		return nil, err
	}

	// Get load info
	loadInfo, err := load.Avg()
	if err != nil {
		return nil, err
	}
	// TODO somebody check my maths here. taking the load average over 5 minutes, which is in the range 0-1 so multiply by 100 and divide by number of CPUs
	sysInfo.CPUUsagePercentage = loadInfo.Load5 * 100 / float64(len(cpuInfo))

	// TODO bandwidth usage

	return sysInfo, nil
}
