package codes

import (
	"math"
	"runtime"

	"github.com/k0new/go-eccodes/debug"
	"github.com/k0new/go-eccodes/native"
)

type Message interface {
	isOpen() bool

	GetString(key string) (string, error)
	GetStringArray(key string) ([]string, error)

	GetLong(key string) (int64, error)
	SetLong(key string, value int64) error

	GetDouble(key string) (float64, error)
	SetDouble(key string, value float64) error
	GetDoubleArray(key string) ([]float64, error)

	Data() (latitudes []float64, longitudes []float64, values []float64, err error)
	DataUnsafe() (latitudes *Float64ArrayUnsafe, longitudes *Float64ArrayUnsafe, values *Float64ArrayUnsafe, err error)

	Close() error

	KeyIterator() Iterator

	Clone() (Message, error)
}

type Iterator interface {
	isOpen() bool
	Next() bool
	GetName() string
	Close() error
}

type iterator struct {
	handle native.Ccodes_handle
}

type message struct {
	handle native.Ccodes_handle
}

func newMessage(h native.Ccodes_handle) Message {
	m := &message{handle: h}
	runtime.SetFinalizer(m, messageFinalizer)

	// set missing value to NaN
	m.SetDouble(parameterMissingValue, math.NaN())

	return m
}

func newIterator(h native.Ccodes_handle) Iterator {
	i := &iterator{handle: h}
	runtime.SetFinalizer(i, iteratorFinalizer)
	return i
}

func (i *iterator) GetName() string {
	return native.Ccodes_keys_iterator_get_name(i.handle)
}

func (i *iterator) Next() bool {
	return native.Ccodes_keys_iterator_next(i.handle) == 1
}

func (i *iterator) isOpen() bool {
	return i.handle != nil
}

func (i *iterator) Close() error {
	defer func() { i.handle = nil }()
	return native.Ccodes_keys_iterator_delete(i.handle)
}

func (m *message) isOpen() bool {
	return m.handle != nil
}

func (m *message) Clone() (Message, error) {
	single, err := native.Ccodes_clone(m.handle)
	if err != nil {
		return nil, err
	}
	return newMessage(single), nil
}

func (m *message) GetString(key string) (string, error) {
	return native.Ccodes_get_string(m.handle, key)
}

func (m *message) KeyIterator() Iterator {
	iter := native.Ccodes_keys_iterator_new(m.handle, 0, "")

	return newIterator(iter)
}

func (m *message) GetLong(key string) (int64, error) {
	return native.Ccodes_get_long(m.handle, key)
}

func (m *message) SetLong(key string, value int64) error {
	return native.Ccodes_set_long(m.handle, key, value)
}

func (m *message) GetDouble(key string) (float64, error) {
	return native.Ccodes_get_double(m.handle, key)
}

func (m *message) GetStringArray(key string) ([]string, error) {
	return native.Ccodes_get_string_array(m.handle, key)
}
func (m *message) GetDoubleArray(key string) ([]float64, error) {
	return native.Ccodes_get_double_array(m.handle, key)
}

func (m *message) SetDouble(key string, value float64) error {
	return native.Ccodes_set_double(m.handle, key, value)
}

func (m *message) Data() (latitudes []float64, longitudes []float64, values []float64, err error) {
	return native.Ccodes_grib_get_data(m.handle)
}

func (m *message) DataUnsafe() (latitudes *Float64ArrayUnsafe, longitudes *Float64ArrayUnsafe, values *Float64ArrayUnsafe, err error) {
	lats, lons, vals, err := native.Ccodes_grib_get_data_unsafe(m.handle)
	if err != nil {
		return nil, nil, nil, err
	}
	return newFloat64ArrayUnsafe(lats), newFloat64ArrayUnsafe(lons), newFloat64ArrayUnsafe(vals), nil
}

func (m *message) Close() error {
	defer func() { m.handle = nil }()
	return native.Ccodes_handle_delete(m.handle)
}

func messageFinalizer(m *message) {
	if m.isOpen() {
		debug.MemoryLeakLogger.Print("message is not closed")
		m.Close()
	}
}

func iteratorFinalizer(i *iterator) {
	if i.isOpen() {
		debug.MemoryLeakLogger.Print("iterator is not closed")
		i.Close()
	}
}
