package utils

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/prasannakumar414/click-replicator/tools"
)

func Contains(slice []string, item string) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

func ConvertToString(val interface{}) string {
	switch v := val.(type) {
	case string:
		return v
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v) // Or strconv.FormatInt(v.(int64), 10)
	case float32, float64:
		return fmt.Sprintf("%f", v) // Or strconv.FormatFloat(v.(float64), 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	case []byte: // Common when reading binary data from database
		return string(v)
	case time.Time: // Often DateTime fields from DBs are time.Time
		return v.Format(time.RFC3339) // Choose your desired time format
	case fmt.Stringer: // If the type implements the fmt.Stringer interface
		return v.String()
	default:
		// Fallback for any other type - use fmt.Sprint
		return fmt.Sprint(v)
	}
}

func GetDataType(val string) string {
	if _, err := strconv.Atoi(val); err == nil {
		return "Int64"
	} else if _, err := strconv.ParseFloat(val, 64); err == nil {
		return "Float64"
	} else if _, err := time.Parse(time.RFC3339, val); err == nil {
		return "DateTime"
	}
	return "String"
}

// get added columns
func GetAddedColumns(oldColumns []string, newColumns []string) []string {
	addedColumns := []string{}
	oldSet := make(map[string]struct{})
	for _, col := range oldColumns {
		oldSet[col] = struct{}{}
	}

	for _, col := range newColumns {
		if _, exists := oldSet[col]; !exists {
			addedColumns = append(addedColumns, col)
		}
	}

	return addedColumns
}

// get all keys of a map

func GetMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// Remove Prefix of a String

func RemovePrefix(s string, prefix string) string {
	if len(s) >= len(prefix) && s[:len(prefix)] == prefix {
		return s[len(prefix):]
	}
	return s
}

// Join strings with a separator
func Join(values []string, separator string) string {
	if len(values) == 0 {
		return ""
	}
	result := values[0]
	for _, value := range values[1:] {
		result += separator + value
	}
	return result
}

func JoinValues(values []string, separator string) string {
	if len(values) == 0 {
		return ""
	}
	result := values[0]
	for _, value := range values[1:] {
		result += separator + "'" + value + "'"
	}
	return result
}

func TransformedValues(dataMap map[string]interface{}) (string, string, error) {
	flattenedMap, err := tools.Flatten(dataMap, "", tools.UnderscoreStyle)
	if err != nil {
		fmt.Println("Error flattening JSON:", err)
		return "", "", err
	}
	values := ""
	columns := ""
	for key, value := range flattenedMap {
		strValue := ConvertToString(value)
		// Escape single quotes in string values
		strValue = strings.ReplaceAll(strValue, "'", "''")
		values += fmt.Sprintf("'%s', ", strValue)
		columns += fmt.Sprintf("%s, ", key)
	}
	values = strings.TrimSuffix(values, ", ")
	columns = strings.TrimSuffix(columns, ", ")
	return fmt.Sprintf("(%s)", columns), fmt.Sprintf("(%s)", values), nil
}

// DeleteJSONKey removes a key from a JSON string and returns the modified JSON string.
func DeleteJSONKey(jsonStr, key string) (string, error) {
	// Parse the JSON string into a map
	var data map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &data)
	if err != nil {
		return "", fmt.Errorf("invalid JSON: %w", err)
	}

	// Delete the key
	delete(data, key)

	// Marshal the map back into a JSON string
	modifiedJSON, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("error marshaling modified JSON: %w", err)
	}

	return string(modifiedJSON), nil
}
