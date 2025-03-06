package cmd

import "fmt"

type EnumFlag[T comparable] struct {
	value     T
	available []EnumValue[T]
}

func (o *EnumFlag[T]) Type() string { return "string" }
func (o *EnumFlag[T]) Get() T       { return o.value }

func (o *EnumFlag[T]) Set(value string) error {
	for _, v := range o.available {
		if v.Name == value {
			o.value = v.Value
			return nil
		}
	}
	return fmt.Errorf("invalid value %q, must be one of %v", value, o.available)
}

func (o *EnumFlag[T]) String() string {
	for _, v := range o.available {
		if v.Value == o.value {
			return v.Name
		}
	}
	return "<unknown>"
}

type EnumValue[T comparable] struct {
	Name  string
	Value T
}

func (o EnumValue[T]) String() string { return o.Name }

func NewEnumFlag[T comparable](value T, available []EnumValue[T]) *EnumFlag[T] {
	return &EnumFlag[T]{
		value:     value,
		available: available,
	}
}
