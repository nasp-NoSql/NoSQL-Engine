package hyperloglog

import (
	"crypto/md5"
	"encoding/binary"
	"errors"
	"math"
	"math/bits"
	"os"
)

type HyperLogLog struct {
	p         uint8   // precision
	m         uint32  // number of registers
	registers []uint8 // register array
}

func CalculateP(errorRate float64) uint8 {
	return uint8(math.Ceil(math.Log2(math.Pow(1.04/errorRate, 2))))
}

func AlphaMM(m uint32) float64 {
	switch m {
	case 16:
		return 0.673 * float64(m*m)
	case 32:
		return 0.697 * float64(m*m)
	case 64:
		return 0.709 * float64(m*m)
	default:
		return (0.7213 / (1 + 1.079/float64(m))) * float64(m*m)
	}
}

func (hll *HyperLogLog) Initialize(errorRate float64) error {
	p := CalculateP(errorRate)
	if p < 4 || p > 16 {
		return errors.New("precision p must be between 4 and 16")
	}
	m := uint32(1) << p
	hll.p = p
	hll.m = m
	hll.registers = make([]uint8, m)
	return nil
}

func (hll *HyperLogLog) hash(data []byte) uint64 {
	h := md5.Sum(data)
	return binary.BigEndian.Uint64(h[:8])
}

func (hll *HyperLogLog) Add(data []byte) {
	hash := hll.hash(data)
	idx := hash >> (64 - hll.p)
	w := hash<<hll.p | (1 << (hll.p - 1))

	zeroBits := uint8(bits.LeadingZeros64(w) + 1)
	if zeroBits > hll.registers[idx] {
		hll.registers[idx] = zeroBits
	}
}

func (hll *HyperLogLog) Estimate() uint64 {
	var sum float64
	for _, reg := range hll.registers {
		sum += 1.0 / math.Pow(2.0, float64(reg))
	}

	alphaMM := AlphaMM(hll.m)
	estimate := alphaMM / sum

	if estimate <= 2.5*float64(hll.m) {
		var zeros float64
		for _, reg := range hll.registers {
			if reg == 0 {
				zeros++
			}
		}
		if zeros != 0 {
			return uint64(float64(hll.m) * math.Log(float64(hll.m)/zeros))
		}
	}

	if estimate > (1.0/30.0)*math.Pow(2, 32) {
		return uint64(-math.Pow(2, 32) * math.Log(1-estimate/math.Pow(2, 32)))
	}

	return uint64(estimate)
}

func (hll *HyperLogLog) Serialize(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := binary.Write(file, binary.BigEndian, hll.p); err != nil {
		return err
	}

	if err := binary.Write(file, binary.BigEndian, hll.m); err != nil {
		return err
	}

	if _, err := file.Write(hll.registers); err != nil {
		return err
	}

	return nil
}

func Deserialize(filename string) (HyperLogLog, error) {
	var hll HyperLogLog

	file, err := os.Open(filename)
	if err != nil {
		return hll, err
	}
	defer file.Close()

	if err := binary.Read(file, binary.BigEndian, &hll.p); err != nil {
		return hll, err
	}

	if err := binary.Read(file, binary.BigEndian, &hll.m); err != nil {
		return hll, err
	}

	hll.registers = make([]uint8, hll.m)
	if _, err := file.Read(hll.registers); err != nil {
		return hll, err
	}

	return hll, nil
}
