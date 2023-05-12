package main

type arrayFlags []string

func (i *arrayFlags) String() string {
	return "string representation of flag"
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}
