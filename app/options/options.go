/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package options

import (
	"bufio"
	"log"
	"os"
	"strings"
)

// Options has all the params needed to run a dqlite
type Options struct {
	StorageDir         string
	ListenEp           string
	EnableTls          bool
}

func NewOptions() (*Options){
	o := Options{
		"/var/tmp/k8s-dqlite",
		"tcp://127.0.0.1:12379",
		true,
	}
	return &o
}

func ReadArgsFromFile(filename string) []string {
	var args []string
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Failed to open arguments file %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSpace(line)
		// ignore lines with # and empty lines
		if len(line) <= 0 || strings.HasPrefix(line, "#") {
			continue
		}
		// remove " and '
		for _, r := range "\"'" {
			line = strings.ReplaceAll(line, string(r), "")
		}
		for _, part := range strings.Split(line, " ") {

			args = append(args, os.ExpandEnv(part))
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("Failed to read arguments file %v", err)
	}
	return args
}
