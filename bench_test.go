package go_pgx_perf_observations_test

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

func BenchmarkNewBuffers(b *testing.B) {
	for i := 0; i < b.N; i++ {
		n := rand.Intn(1024)
		buf := make([]byte, n)

		// Do something with buffer
		for j := 0; j < n; j++ {
			buf[j] = 1
		}
	}
}

func BenchmarkReuseBuffers(b *testing.B) {
	sharedBuf := make([]byte, 1024)

	for i := 0; i < b.N; i++ {
		n := rand.Intn(1024)
		buf := sharedBuf[0:n]

		// Do something with buffer
		for j := 0; j < n; j++ {
			buf[j] = 1
		}
	}
}

func BenchmarkUnbufferedFileWrite(b *testing.B) {
	file, err := os.Create("unbuffered.test")
	if err != nil {
		b.Fatalf("Unable to create file: %v", err)
	}
	defer func() {
		file.Close()
		os.Remove(file.Name())
	}()

	for i := 0; i < b.N; i++ {
		fmt.Fprintln(file, "Hello world")
	}
}

func BenchmarkBufferedFileWrite(b *testing.B) {
	file, err := os.Create("buffered.test")
	if err != nil {
		b.Fatalf("Unable to create file: %v", err)
	}
	defer func() {
		file.Close()
		os.Remove(file.Name())
	}()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	for i := 0; i < b.N; i++ {
		fmt.Fprintln(writer, "Hello world")
	}
}

func BenchmarkParseInt32Text(b *testing.B) {
	s := "12345678"
	expected := int32(12345678)

	for i := 0; i < b.N; i++ {
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			b.Fatalf("strconv.ParseInt failed: %v", err)
		}
		if int32(n) != expected {
			b.Fatalf("strconv.ParseInt decoded %v instead of %v", n, expected)
		}
	}
}

func BenchmarkParseInt32Binary(b *testing.B) {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, 12345678)
	expected := int32(12345678)

	for i := 0; i < b.N; i++ {
		n := int32(binary.BigEndian.Uint32(buf))
		if n != expected {
			b.Fatalf("Got %v instead of %v", n, expected)
		}
	}
}

func BenchmarkParseTimeText(b *testing.B) {
	s := "2011-10-25 09:12:34.345921-05"
	expected, _ := time.Parse("2006-01-02 15:04:05.999999-07", s)

	for i := 0; i < b.N; i++ {
		t, err := time.Parse("2006-01-02 15:04:05.999999-07", s)
		if err != nil {
			b.Fatalf("time.Parse failed: %v", err)
		}
		if t != expected {
			b.Fatalf("time.Parse decoded %v instead of %v", t, expected)
		}
	}
}

// PostgreSQL binary format is an int64 of the number of microseconds since Y2K
func BenchmarkParseTimeBinary(b *testing.B) {
	microsecFromUnixEpochToY2K := int64(946684800 * 1000000)

	s := "2011-10-25 09:12:34.345921-05"
	expected, _ := time.Parse("2006-01-02 15:04:05.999999-07", s)

	microsecSinceUnixEpoch := expected.Unix()*1000000 + int64(expected.Nanosecond())/1000
	microsecSinceY2K := microsecSinceUnixEpoch - microsecFromUnixEpochToY2K

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(microsecSinceY2K))

	for i := 0; i < b.N; i++ {
		microsecSinceY2K := int64(binary.BigEndian.Uint64(buf))
		microsecSinceUnixEpoch := microsecFromUnixEpochToY2K + microsecSinceY2K
		t := time.Unix(microsecSinceUnixEpoch/1000000, (microsecSinceUnixEpoch%1000000)*1000)
		if t != expected {
			b.Fatalf("Got %v instead of %v", t, expected)
		}
	}
}

func BenchmarkBinaryWrite(b *testing.B) {
	buf := &bytes.Buffer{}

	for i := 0; i < b.N; i++ {
		buf.Reset()

		for j := 0; j < 10; j++ {
			binary.Write(buf, binary.BigEndian, int32(j))
		}
	}
}

func BenchmarkBinaryPut(b *testing.B) {
	var writebuf [1024]byte

	for i := 0; i < b.N; i++ {
		buf := writebuf[0:0]

		for j := 0; j < 10; j++ {
			b := make([]byte, 4)
			binary.BigEndian.PutUint32(b, uint32(j))
			buf = append(buf, b...)
		}
	}
}
