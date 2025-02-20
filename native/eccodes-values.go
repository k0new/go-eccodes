package native

/*
#include <eccodes.h>
*/
import "C"
import (
	"io"
	"reflect"
	"unsafe"

	"github.com/pkg/errors"

	"github.com/k0new/go-eccodes/debug"
)

const MaxStringLength = 1030
const ParameterNumberOfPoints = "numberOfDataPoints"

func Ccodes_get_long(handle Ccodes_handle, key string) (int64, error) {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	var value Clong
	cValue := (*C.long)(unsafe.Pointer(&value))
	err := C.codes_get_long((*C.codes_handle)(handle), cKey, cValue)
	if err != 0 {
		return 0, errors.New(Cgrib_get_error_message(int(err)))
	}

	return int64(value), nil
}

func Ccodes_clone(handle Ccodes_handle) (Ccodes_handle, error) {
	h, err := C.codes_handle_clone((*C.codes_handle)(handle))
	//if err != 0 {
	//	return nil, errors.New(Cgrib_get_error_message(int(err)))
	//}
	if h == nil {
		return nil, io.EOF
	}

	return unsafe.Pointer(h), err
}

func Ccodes_set_long(handle Ccodes_handle, key string, value int64) error {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	err := C.codes_set_long((*C.codes_handle)(handle), cKey, C.long(Clong(value)))
	if err != 0 {
		return errors.New(Cgrib_get_error_message(int(err)))
	}

	return nil
}

func Ccodes_get_double(handle Ccodes_handle, key string) (float64, error) {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	var value Cdouble
	cValue := (*C.double)(unsafe.Pointer(&value))
	err := C.codes_get_double((*C.codes_handle)(handle), cKey, cValue)
	if err != 0 {
		return 0, errors.New(Cgrib_get_error_message(int(err)))
	}

	return float64(value), nil
}

func Ccodes_set_double(handle Ccodes_handle, key string, value float64) error {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	err := C.codes_set_double((*C.codes_handle)(handle), cKey, C.double(Cdouble(value)))
	if err != 0 {
		return errors.New(Cgrib_get_error_message(int(err)))
	}

	return nil
}

func freeStringArray(strArray **C.char, len C.int) {
	if strArray == nil {
		return
	}

	sliceHeader := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(strArray)),
		Len:  int(len),
		Cap:  int(len),
	}
	slice := *(*[]*C.char)(unsafe.Pointer(&sliceHeader))

	for i := range slice {
		C.free(unsafe.Pointer(slice[i]))
	}

	C.free(unsafe.Pointer(strArray))
}

func Ccodes_get_string_array(handle Ccodes_handle, key string) ([]string, error) {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	var err C.int
	var size int
	cSize := (*C.size_t)(unsafe.Pointer(&size))

	//var strArr []string
	//cArr := (**C.char)(unsafe.Pointer(&strArr[0]))

	err = C.codes_get_size((*C.codes_handle)(handle), cKey, cSize)
	if err != 0 {
		return nil, errors.New(Cgrib_get_error_message(int(err)))
	}
	strArray := (**C.char)(C.malloc(C.size_t(*cSize) * C.size_t(unsafe.Sizeof((*C.char)(nil)))))
	if strArray == nil {
		return nil, errors.New("Error: Could not allocate memory")
	}

	err = C.codes_get_string_array((*C.codes_handle)(handle), cKey, strArray, cSize)
	if err != 0 {
		return nil, errors.New(Cgrib_get_error_message(int(err)))
	}

	//length := CsizeT(MaxStringLength)
	//cLength := (*C.size_t)(unsafe.Pointer(&length))
	//
	//err := C.codes_get_length((*C.codes_handle)(handle), cKey, cLength)
	//if err != 0 {
	//	return nil, errors.New(Cgrib_get_error_message(int(err)))
	//}
	//// +1 byte for '\0'
	//length++
	//
	//var cBytes *C.char
	//var result []byte
	//
	//if length > MaxStringLength {
	//	debug.MemoryLeakLogger.Printf("unnecessary memory allocation - length of '%s' value is %d greater than MaxStringLength=%d",
	//		key, int(length), MaxStringLength)
	//	result = make([]byte, length)
	//} else {
	//	var buffer [MaxStringLength]byte
	//	result = buffer[:]
	//}
	//
	//cBytes = (*C.char)(unsafe.Pointer(&result[0]))
	//err = C.codes_get_string_array((*C.codes_handle)(handle), cKey, cBytes, cLength)
	//if err != 0 {
	//	return "", errors.New(Cgrib_get_error_message(int(err)))
	//}
	//
	//if length == 0 {
	//	return "", nil
	//}
	//return string(result[:length-1]), nil\
	goStrings := make([]string, int(*cSize))
	charSize := int(unsafe.Sizeof((*C.char)(nil)))
	for i := 0; i < len(goStrings); i++ {
		str := C.GoString((*C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(*strArray)) + uintptr(i)*uintptr(charSize))))
		goStrings[i] = str
	}

	// Free allocated memory for the string array
	C.free(unsafe.Pointer(strArray))
	return goStrings, nil
	//// +1 byte for '\0'
	//length++
	//
	//var cBytes *C.char
	//var result []byte
	//
	//if length > MaxStringLength {
	//	debug.MemoryLeakLogger.Printf("unnecessary memory allocation - length of '%s' value is %d greater than MaxStringLength=%d",
	//		key, int(length), MaxStringLength)
	//	result = make([]byte, length)
	//} else {
	//	var buffer [MaxStringLength]byte
	//	result = buffer[:]
	//}
	//
	//cBytes = (*C.char)(unsafe.Pointer(&result[0]))
	//err = C.codes_get_string((*C.codes_handle)(handle), cKey, cBytes, cLength)
	//if err != 0 {
	//	return "", errors.New(Cgrib_get_error_message(int(err)))
	//}
	//
	//if length == 0 {
	//	return "", nil
	//}
	//return string(result[:length-1]), nil
}

func Ccodes_get_double_array(handle Ccodes_handle, key string) ([]float64, error) {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	var err C.int
	var size int
	cSize := (*C.size_t)(unsafe.Pointer(&size))

	err = C.codes_get_size((*C.codes_handle)(handle), cKey, cSize)
	if err != 0 {
		return nil, errors.New(Cgrib_get_error_message(int(err)))
	}
	res := make([]float64, size)
	cRes := (*C.double)(unsafe.Pointer(&res[0]))

	err = C.codes_get_double_array((*C.codes_handle)(handle), cKey, cRes, cSize)
	if err != 0 {
		return nil, errors.New(Cgrib_get_error_message(int(err)))
	}

	return res, nil
}

func Ccodes_get_string(handle Ccodes_handle, key string) (string, error) {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	length := CsizeT(MaxStringLength)
	cLength := (*C.size_t)(unsafe.Pointer(&length))

	err := C.codes_get_length((*C.codes_handle)(handle), cKey, cLength)
	if err != 0 {
		return "", errors.New(Cgrib_get_error_message(int(err)))
	}
	// +1 byte for '\0'
	length++

	var cBytes *C.char
	var result []byte

	if length > MaxStringLength {
		debug.MemoryLeakLogger.Printf("unnecessary memory allocation - length of '%s' value is %d greater than MaxStringLength=%d",
			key, int(length), MaxStringLength)
		result = make([]byte, length)
	} else {
		var buffer [MaxStringLength]byte
		result = buffer[:]
	}

	cBytes = (*C.char)(unsafe.Pointer(&result[0]))
	err = C.codes_get_string((*C.codes_handle)(handle), cKey, cBytes, cLength)
	if err != 0 {
		return "", errors.New(Cgrib_get_error_message(int(err)))
	}

	if length == 0 {
		return "", nil
	}
	return string(result[:length-1]), nil
}

func Ccodes_grib_get_data(handle Ccodes_handle) (latitudes []float64, longitudes []float64, values []float64, err error) {

	size, err := Ccodes_get_long(handle, ParameterNumberOfPoints)
	if err != nil {
		return nil, nil, nil, errors.Wrapf(err, "failed to get long value of '%s'", ParameterNumberOfPoints)
	}

	latitudes = make([]float64, size)
	cLatitudes := (*C.double)(unsafe.Pointer(&latitudes[0]))

	longitudes = make([]float64, size)
	cLongitudes := (*C.double)(unsafe.Pointer(&longitudes[0]))

	values = make([]float64, size)
	cValues := (*C.double)(unsafe.Pointer(&values[0]))

	res := C.codes_grib_get_data((*C.codes_handle)(handle), cLatitudes, cLongitudes, cValues)
	if res != 0 {
		return nil, nil, nil, errors.New(Cgrib_get_error_message(int(res)))
	}

	return latitudes, longitudes, values, nil
}

func Ccodes_grib_get_data_unsafe(handle Ccodes_handle) (latitudes unsafe.Pointer, longitudes unsafe.Pointer, values unsafe.Pointer, err error) {

	size, err := Ccodes_get_long(handle, ParameterNumberOfPoints)
	if err != nil {
		return nil, nil, nil, errors.Wrapf(err, "failed to get long value of '%s'", ParameterNumberOfPoints)
	}

	latitudes = Cmalloc(CsizeT(size * SizeOfFloat64))
	cLatitudes := (*C.double)(latitudes)

	longitudes = Cmalloc(CsizeT(size * SizeOfFloat64))
	cLongitudes := (*C.double)(longitudes)

	values = Cmalloc(CsizeT(size * SizeOfFloat64))
	cValues := (*C.double)(values)

	res := C.codes_grib_get_data((*C.codes_handle)(handle), cLatitudes, cLongitudes, cValues)
	if res != 0 {
		Cfree(latitudes)
		Cfree(longitudes)
		Cfree(values)
		return nil, nil, nil, errors.New(Cgrib_get_error_message(int(res)))
	}

	return latitudes, longitudes, values, nil
}
