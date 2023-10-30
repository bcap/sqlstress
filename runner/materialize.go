package runner

import (
	"strings"
	"sync"

	"github.com/bcap/sqlstress/config"
)

var commandVarsCacheLock sync.RWMutex
var commandVarsCache map[string][]string = map[string][]string{}

func materializeCommand(command string, vars []config.QueryVar, randomWeights [][]int, randomSource <-chan int64) string {
	// This uses a cache structure to avoid wasting time trying to do useless string replacements
	commandVarsCacheLock.RLock()
	varsUsed, hasOnCache := commandVarsCache[command]
	commandVarsCacheLock.RUnlock()

	// Check if we cached that the command does not use variable replacements. If so, do an early return
	if hasOnCache && len(varsUsed) == 0 {
		return command
	}

	// Pick the variable values to be used.
	// Respect weighted randomization when there are multiple possible values for a var
	values := map[string]string{}
	for idx, queryVar := range vars {
		// query var has only one possible value defined directly on the Value property
		if queryVar.Value != "" {
			values[queryVar.Key] = queryVar.Value
			continue
		}
		// query var has only one possible value defined as an array of values with a single item
		if len(queryVar.Values) == 1 {
			values[queryVar.Key] = queryVar.Values[0].Value
			continue
		}

		// query var has multiple possible values. Lets pick at random while respecting the weighted random mechanism
		varWeightIdx := randomWeights[idx]
		rand64 := <-randomSource
		randIdx := int(rand64 % int64(len(varWeightIdx)))
		chosenIdx := varWeightIdx[randIdx]
		chosen := queryVar.Values[chosenIdx]
		values[queryVar.Key] = chosen.Value
	}

	if hasOnCache {
		for _, key := range varsUsed {
			value := values[key]
			command = strings.ReplaceAll(command, "{{"+key+"}}", value)
		}
	} else {
		varsUsed := []string{}
		for key, value := range values {
			templateKey := "{{" + key + "}}"
			if !strings.Contains(command, templateKey) {
				continue
			}
			command = strings.ReplaceAll(command, templateKey, value)
			varsUsed = append(varsUsed, key)
		}
		commandVarsCacheLock.Lock()
		commandVarsCache[command] = varsUsed
		commandVarsCacheLock.Unlock()
	}

	return command
}
