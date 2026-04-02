package utils

import (
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

func GenerateUUID() (string, error) {
	uuid := make([]byte, 16)
	n, err := rand.Read(uuid)
	if err != nil {
		return "", fmt.Errorf("failed to generate random bytes: %w", err)
	}
	if n != 16 {
		return "", fmt.Errorf("failed to generate enough random bytes")
	}

	uuid[8] = uuid[8]&0x3f | 0x80
	uuid[6] = uuid[6]&0x0f | 0x40

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16]), nil
}

func GenerateShortUUID() (string, error) {
	uuid := make([]byte, 12)
	n, err := rand.Read(uuid)
	if err != nil {
		return "", fmt.Errorf("failed to generate random bytes: %w", err)
	}
	if n != 12 {
		return "", fmt.Errorf("failed to generate enough random bytes")
	}

	return base64.URLEncoding.EncodeToString(uuid)[:16], nil
}

func MD5Hash(data string) string {
	h := md5.New()
	h.Write([]byte(data))
	return hex.EncodeToString(h.Sum(nil))
}

func MD5HashBytes(data []byte) string {
	h := md5.New()
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

func SHA256Hash(data string) string {
	h := sha256.New()
	h.Write([]byte(data))
	return hex.EncodeToString(h.Sum(nil))
}

func SHA256HashBytes(data []byte) string {
	h := sha256.New()
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

func Now() time.Time {
	return time.Now()
}

func NowUTC() time.Time {
	return time.Now().UTC()
}

func NowUnix() int64 {
	return time.Now().Unix()
}

func NowUnixMilli() int64 {
	return time.Now().UnixMilli()
}

func NowUnixNano() int64 {
	return time.Now().UnixNano()
}

func FormatTime(t time.Time, layout string) string {
	if layout == "" {
		layout = time.RFC3339
	}
	return t.Format(layout)
}

func FormatTimeDefault(t time.Time) string {
	return t.Format(time.RFC3339)
}

func FormatTimeISO(t time.Time) string {
	return t.Format("2006-01-02T15:04:05Z")
}

func FormatDate(t time.Time) string {
	return t.Format("2006-01-02")
}

func FormatDateTime(t time.Time) string {
	return t.Format("2006-01-02 15:04:05")
}

func ParseTime(s string, layout string) (time.Time, error) {
	if layout == "" {
		layout = time.RFC3339
	}
	return time.Parse(layout, s)
}

func ParseTimeDefault(s string) (time.Time, error) {
	return time.Parse(time.RFC3339, s)
}

func ParseTimeInLocation(s string, layout string, loc *time.Location) (time.Time, error) {
	if layout == "" {
		layout = time.RFC3339
	}
	return time.ParseInLocation(layout, s, loc)
}

func ParseDate(s string) (time.Time, error) {
	return time.Parse("2006-01-02", s)
}

func ParseDateTime(s string) (time.Time, error) {
	return time.Parse("2006-01-02 15:04:05", s)
}

func DurationBetween(start, end time.Time) time.Duration {
	return end.Sub(start)
}

func DurationSeconds(d time.Duration) float64 {
	return d.Seconds()
}

func DurationMilliseconds(d time.Duration) int64 {
	return d.Milliseconds()
}

func AddDate(year, month, day int) time.Time {
	return time.Now().AddDate(year, month, day)
}

func AddDuration(d time.Duration) time.Time {
	return time.Now().Add(d)
}

func SubtractDuration(d time.Duration) time.Time {
	return time.Now().Add(-d)
}

func BeginOfDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}

func EndOfDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 23, 59, 59, 999999999, t.Location())
}

func BeginOfWeek(t time.Time) time.Time {
	weekday := int(t.Weekday())
	if weekday == 0 {
		weekday = 7
	}
	return BeginOfDay(t).AddDate(0, 0, -(weekday - 1))
}

func EndOfWeek(t time.Time) time.Time {
	return BeginOfWeek(t).AddDate(0, 0, 7).Add(-time.Nanosecond)
}

func BeginOfMonth(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
}

func EndOfMonth(t time.Time) time.Time {
	return BeginOfMonth(t).AddDate(0, 1, 0).Add(-time.Nanosecond)
}

func BeginOfYear(t time.Time) time.Time {
	return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location())
}

func EndOfYear(t time.Time) time.Time {
	return time.Date(t.Year(), 12, 31, 23, 59, 59, 999999999, t.Location())
}

func Age(from time.Time) time.Duration {
	return time.Since(from)
}

func AgeInSeconds(from time.Time) float64 {
	return time.Since(from).Seconds()
}

func AgeInMinutes(from time.Time) float64 {
	return time.Since(from).Minutes()
}

func AgeInHours(from time.Time) float64 {
	return time.Since(from).Hours()
}

func AgeInDays(from time.Time) float64 {
	return time.Since(from).Hours() / 24
}

func IsToday(t time.Time) bool {
	now := time.Now()
	return t.Year() == now.Year() && t.Month() == now.Month() && t.Day() == now.Day()
}

func IsYesterday(t time.Time) bool {
	yesterday := time.Now().AddDate(0, 0, -1)
	return t.Year() == yesterday.Year() && t.Month() == yesterday.Month() && t.Day() == yesterday.Day()
}

func IsTomorrow(t time.Time) bool {
	tomorrow := time.Now().AddDate(0, 0, 1)
	return t.Year() == tomorrow.Year() && t.Month() == tomorrow.Month() && t.Day() == tomorrow.Day()
}

func IsSameDay(t1, t2 time.Time) bool {
	return t1.Year() == t2.Year() && t1.Month() == t2.Month() && t1.Day() == t2.Day()
}

func DaysBetween(start, end time.Time) int {
	return int(end.Sub(start).Hours() / 24)
}

func GetExecutablePath() (string, error) {
	exePath, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("failed to get executable path: %w", err)
	}
	return filepath.Dir(exePath), nil
}

func GetExecutableDir() (string, error) {
	return GetExecutablePath()
}

func GetWorkingDir() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("failed to get working directory: %w", err)
	}
	return cwd, nil
}

func GetTempDir() string {
	return os.TempDir()
}

func GetHomeDir() (string, error) {
	home := os.Getenv("HOME")
	if home != "" {
		return home, nil
	}
	home = os.Getenv("USERPROFILE")
	if home != "" {
		return home, nil
	}
	return "", fmt.Errorf("failed to get home directory")
}

func PathJoin(elem ...string) string {
	return filepath.Join(elem...)
}

func PathDir(path string) string {
	return filepath.Dir(path)
}

func PathBase(path string) string {
	return filepath.Base(path)
}

func PathExt(path string) string {
	return filepath.Ext(path)
}

func PathClean(path string) string {
	return filepath.Clean(path)
}

func PathAbs(path string) (string, error) {
	return filepath.Abs(path)
}

func PathRel(basepath, targpath string) (string, error) {
	return filepath.Rel(basepath, targpath)
}

func FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func DirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func IsAbsPath(path string) bool {
	return filepath.IsAbs(path)
}

func MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func MkdirTemp(prefix string) (string, error) {
	return os.MkdirTemp("", prefix)
}

func RemoveAll(path string) error {
	return os.RemoveAll(path)
}

func Rename(oldPath, newPath string) error {
	return os.Rename(oldPath, newPath)
}

func CopyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	if err != nil {
		return err
	}

	sourceInfo, err := os.Stat(src)
	if err != nil {
		return err
	}

	return os.Chmod(dst, sourceInfo.Mode())
}

func ReadFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func WriteFile(path string, data []byte, perm os.FileMode) error {
	return os.WriteFile(path, data, perm)
}

func AppendFile(path string, data []byte, perm os.FileMode) error {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, perm)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(data)
	return err
}

func GetFileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func GetFileModTime(path string) (time.Time, error) {
	info, err := os.Stat(path)
	if err != nil {
		return time.Time{}, err
	}
	return info.ModTime(), nil
}

func ListFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			files = append(files, entry.Name())
		}
	}
	return files, nil
}

func ListDirs(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	dirs := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			dirs = append(dirs, entry.Name())
		}
	}
	return dirs, nil
}

func WalkDir(root string, fn filepath.WalkFunc) error {
	return filepath.Walk(root, fn)
}

func StringToPtr(s string) *string {
	return &s
}

func PtrToString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func IntToPtr(i int) *int {
	return &i
}

func PtrToInt(i *int) int {
	if i == nil {
		return 0
	}
	return *i
}

func Int64ToPtr(i int64) *int64 {
	return &i
}

func PtrToInt64(i *int64) int64 {
	if i == nil {
		return 0
	}
	return *i
}

func BoolToPtr(b bool) *bool {
	return &b
}

func PtrToBool(b *bool) bool {
	if b == nil {
		return false
	}
	return *b
}

func StringOrDefault(s, defaultVal string) string {
	if s == "" {
		return defaultVal
	}
	return s
}

func IntOrDefault(i, defaultVal int) int {
	if i == 0 {
		return defaultVal
	}
	return i
}

func StringsContains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func StringsUnique(slice []string) []string {
	seen := make(map[string]struct{})
	result := make([]string, 0, len(slice))
	for _, s := range slice {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			result = append(result, s)
		}
	}
	return result
}

func StringsFilter(slice []string, fn func(string) bool) []string {
	result := make([]string, 0, len(slice))
	for _, s := range slice {
		if fn(s) {
			result = append(result, s)
		}
	}
	return result
}

func StringsMap(slice []string, fn func(string) string) []string {
	result := make([]string, len(slice))
	for i, s := range slice {
		result[i] = fn(s)
	}
	return result
}

func StringsJoin(slice []string, sep string) string {
	return strings.Join(slice, sep)
}

func StringsSplit(s, sep string) []string {
	return strings.Split(s, sep)
}

func StringsSplitN(s, sep string, n int) []string {
	return strings.SplitN(s, sep, n)
}

func StringsTrim(s string) string {
	return strings.TrimSpace(s)
}

func StringsTrimLeft(s string, cutset string) string {
	return strings.TrimLeft(s, cutset)
}

func StringsTrimRight(s string, cutset string) string {
	return strings.TrimRight(s, cutset)
}

func StringsToLower(s string) string {
	return strings.ToLower(s)
}

func StringsToUpper(s string) string {
	return strings.ToUpper(s)
}

func StringsHasPrefix(s, prefix string) bool {
	return strings.HasPrefix(s, prefix)
}

func StringsHasSuffix(s, suffix string) bool {
	return strings.HasSuffix(s, suffix)
}

func StringsReplace(s, old, new string) string {
	return strings.ReplaceAll(s, old, new)
}

func StringsReplaceAll(s, old, new string) string {
	return strings.ReplaceAll(s, old, new)
}

func StringsCount(s, substr string) int {
	return strings.Count(s, substr)
}

func StringsContainsAny(s, chars string) bool {
	return strings.ContainsAny(s, chars)
}

func StringsContainsFunc(s string, f func(rune) bool) bool {
	return strings.ContainsFunc(s, f)
}

func StringToInt(s string) (int, error) {
	return strconv.Atoi(s)
}

func StringToInt64(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

func StringToFloat(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}

func StringToBool(s string) (bool, error) {
	return strconv.ParseBool(s)
}

func IntToString(i int) string {
	return strconv.Itoa(i)
}

func Int64ToString(i int64) string {
	return strconv.FormatInt(i, 10)
}

func FloatToString(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func BoolToString(b bool) string {
	return strconv.FormatBool(b)
}

func StringToBytes(s string) []byte {
	return []byte(s)
}

func BytesToString(b []byte) string {
	return string(b)
}

func StringToRunes(s string) []rune {
	return []rune(s)
}

func RunesToString(r []rune) string {
	return string(r)
}

func StringLength(s string) int {
	return len(s)
}

func StringRuneLength(s string) int {
	return utf8.RuneCountInString(s)
}

func StringIsEmpty(s string) bool {
	return len(strings.TrimSpace(s)) == 0
}

func StringIsBlank(s string) bool {
	return strings.TrimSpace(s) == ""
}

func StringIsNotBlank(s string) bool {
	return !StringIsBlank(s)
}

func StringTruncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

func StringLeftPad(s string, padLen int, padChar string) string {
	if len(s) >= padLen {
		return s
	}
	padding := strings.Repeat(padChar, padLen-len(s))
	return padding + s
}

func StringRightPad(s string, padLen int, padChar string) string {
	if len(s) >= padLen {
		return s
	}
	padding := strings.Repeat(padChar, padLen-len(s))
	return s + padding
}

var emailRegex = regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)

func IsValidEmail(s string) bool {
	return emailRegex.MatchString(s)
}

var urlRegex = regexp.MustCompile(`^https?://[^\s/$.?#].[^\s]*$`)

func IsValidURL(s string) bool {
	return urlRegex.MatchString(s)
}

var uuidRegex = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func IsValidUUID(s string) bool {
	return uuidRegex.MatchString(s)
}

func IsUpper(s string) bool {
	for _, r := range s {
		if !unicode.IsUpper(r) && unicode.IsLetter(r) {
			return false
		}
	}
	return true
}

func IsLower(s string) bool {
	for _, r := range s {
		if !unicode.IsLower(r) && unicode.IsLetter(r) {
			return false
		}
	}
	return true
}

func IsDigit(s string) bool {
	for _, r := range s {
		if !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

func IsAlpha(s string) bool {
	for _, r := range s {
		if !unicode.IsLetter(r) {
			return false
		}
	}
	return true
}

func IsAlphaNumeric(s string) bool {
	for _, r := range s {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

func ReverseString(s string) string {
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

func CamelCase(s string) string {
	words := strings.Fields(s)
	if len(words) == 0 {
		return ""
	}
	result := strings.ToLower(words[0])
	for _, word := range words[1:] {
		result += strings.ToUpper(word[:1]) + strings.ToLower(word[1:])
	}
	return result
}

func SnakeCase(s string) string {
	var result strings.Builder
	for i, r := range s {
		if unicode.IsUpper(r) {
			if i > 0 {
				result.WriteByte('_')
			}
			result.WriteRune(unicode.ToLower(r))
		} else {
			result.WriteRune(r)
		}
	}
	return result.String()
}

func PascalCase(s string) string {
	words := strings.Fields(s)
	if len(words) == 0 {
		return ""
	}
	result := ""
	for _, word := range words {
		result += strings.ToUpper(word[:1]) + strings.ToLower(word[1:])
	}
	return result
}

func KebabCase(s string) string {
	return strings.ReplaceAll(SnakeCase(s), "_", "-")
}

func ToSnakeCase(s string) string {
	return SnakeCase(s)
}

func ToCamelCase(s string) string {
	return CamelCase(s)
}

func ToPascalCase(s string) string {
	return PascalCase(s)
}

func ToKebabCase(s string) string {
	return KebabCase(s)
}

func StructToMap(v interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	elem := reflect.ValueOf(v).Elem()
	typeOf := elem.Type()

	for i := 0; i < elem.NumField(); i++ {
		field := typeOf.Field(i)
		fieldValue := elem.Field(i)
		result[field.Name] = fieldValue.Interface()
	}

	return result
}

func StructToMapString(v interface{}) map[string]string {
	result := make(map[string]string)
	elem := reflect.ValueOf(v).Elem()
	typeOf := elem.Type()

	for i := 0; i < elem.NumField(); i++ {
		field := typeOf.Field(i)
		fieldValue := elem.Field(i)
		result[field.Name] = fmt.Sprintf("%v", fieldValue.Interface())
	}

	return result
}

func CloneMap(m map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{}, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

func CloneStringMap(m map[string]string) map[string]string {
	result := make(map[string]string, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

func MergeMaps(m1, m2 map[string]interface{}) map[string]interface{} {
	result := CloneMap(m1)
	for k, v := range m2 {
		result[k] = v
	}
	return result
}

func MergeStringMaps(m1, m2 map[string]string) map[string]string {
	result := CloneStringMap(m1)
	for k, v := range m2 {
		result[k] = v
	}
	return result
}

func MapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func MapValues(m map[string]interface{}) []interface{} {
	values := make([]interface{}, 0, len(m))
	for _, v := range m {
		values = append(values, v)
	}
	return values
}

func MapHasKey(m map[string]interface{}, key string) bool {
	_, ok := m[key]
	return ok
}

func MapDeleteKey(m map[string]interface{}, key string) {
	delete(m, key)
}

func MapClear(m map[string]interface{}) {
	for k := range m {
		delete(m, k)
	}
}

func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func MaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func MinInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func MaxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func MinFloat64(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func MaxFloat64(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func AbsInt(a int) int {
	if a < 0 {
		return -a
	}
	return a
}

func AbsInt64(a int64) int64 {
	if a < 0 {
		return -a
	}
	return a
}

func AbsFloat64(a float64) float64 {
	if a < 0 {
		return -a
	}
	return a
}

func ClampInt(val, min, max int) int {
	if val < min {
		return min
	}
	if val > max {
		return max
	}
	return val
}

func ClampInt64(val, min, max int64) int64 {
	if val < min {
		return min
	}
	if val > max {
		return max
	}
	return val
}

func ClampFloat64(val, min, max float64) float64 {
	if val < min {
		return min
	}
	if val > max {
		return max
	}
	return val
}

func Round(x float64) float64 {
	return float64(int(x + 0.5))
}

func RoundToDecimalPlaces(x float64, places int) float64 {
	multiplier := 1.0
	for i := 0; i < places; i++ {
		multiplier *= 10
	}
	return float64(int(x*multiplier+0.5)) / multiplier
}

func CopyBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func CompareBytes(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

func BytesToHexString(b []byte) string {
	return hex.EncodeToString(b)
}

func HexStringToBytes(s string) ([]byte, error) {
	return hex.DecodeString(s)
}

func BytesToBase64String(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

func Base64StringToBytes(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(s)
}

func BytesToURLBase64String(b []byte) string {
	return base64.URLEncoding.EncodeToString(b)
}

func URLBase64StringToBytes(s string) ([]byte, error) {
	return base64.URLEncoding.DecodeString(s)
}

type Closer func() error

func NewMultiCloser(closers ...Closer) Closer {
	return func() error {
		var errs []error
		for _, closer := range closers {
			if err := closer(); err != nil {
				errs = append(errs, err)
			}
		}
		if len(errs) > 0 {
			return fmt.Errorf("close errors: %v", errs)
		}
		return nil
	}
}

type OnceValue struct {
	value interface{}
	err   error
	done  bool
	mu    sync.Mutex
}

func (o *OnceValue) Do(fn func() (interface{}, error)) (interface{}, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.done {
		return o.value, o.err
	}
	o.value, o.err = fn()
	o.done = true
	return o.value, o.err
}
