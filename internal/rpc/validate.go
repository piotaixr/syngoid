package rpc

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/piotaixr/syngoid/internal/zfs"
)

var (
	componentRE   = regexp.MustCompile(`^[A-Za-z0-9_.: -]+$`)
	resumeTokenRE = regexp.MustCompile(`^[0-9A-Za-z][0-9A-Za-z-]*$`)
	propNameRE    = regexp.MustCompile(`^[a-z0-9_.:][a-z0-9_.:-]*$`)
)

// recvFlags are the argument-less zfs receive flags a RecvSpec may carry.
var recvFlags = map[string]bool{"-s": true, "-F": true, "-u": true, "-h": true, "-v": true}

// Validate checks the decoded params of a request against the argv-injection
// rules (§7.3), so that no name coming from the wire can become a zfs flag.
// params is the params struct for method, by value or by pointer. It returns
// an *InvalidError naming the first bad field (by its JSON key, dotted for
// nested spec fields), or nil. An unknown method, including hello which is
// not validated here, or params of the wrong type is an InvalidError on
// "params".
func Validate(method string, params any) error {
	switch method {
	case MethodListTree, MethodListSnapshots, MethodListBookmarks, MethodExists,
		MethodLocalProps, MethodAllow, MethodAbortResume, MethodDestroyDataset:
		p, ok := paramsAs[DatasetParams](params)
		if !ok {
			return badParams(params)
		}
		return validDataset("dataset", p.Dataset)
	case MethodGetProps:
		p, ok := paramsAs[GetPropsParams](params)
		if !ok {
			return badParams(params)
		}
		if err := validDataset("dataset", p.Dataset); err != nil {
			return err
		}
		for _, name := range p.Props {
			if !propNameRE.MatchString(name) {
				return &InvalidError{Field: "props", Value: name}
			}
		}
	case MethodSnapshot:
		p, ok := paramsAs[SnapshotParams](params)
		if !ok {
			return badParams(params)
		}
		for _, name := range p.Names {
			if !validJoined(name, '@') {
				return &InvalidError{Field: "names", Value: name}
			}
		}
	case MethodDestroy:
		p, ok := paramsAs[DestroyParams](params)
		if !ok {
			return badParams(params)
		}
		if err := validDataset("dataset", p.Dataset); err != nil {
			return err
		}
		for _, s := range p.Snaps {
			if !validShort(s) {
				return &InvalidError{Field: "snaps", Value: s}
			}
		}
	case MethodHold, MethodRelease:
		p, ok := paramsAs[HoldParams](params)
		if !ok {
			return badParams(params)
		}
		if !validTag(p.Tag) {
			return &InvalidError{Field: "tag", Value: p.Tag}
		}
		if !validJoined(p.Snapshot, '@') {
			return &InvalidError{Field: "snapshot", Value: p.Snapshot}
		}
	case MethodBookmark:
		p, ok := paramsAs[BookmarkParams](params)
		if !ok {
			return badParams(params)
		}
		if !validJoined(p.Snapshot, '@') {
			return &InvalidError{Field: "snapshot", Value: p.Snapshot}
		}
		if !validJoined(p.Bookmark, '#') {
			return &InvalidError{Field: "bookmark", Value: p.Bookmark}
		}
	case MethodSendEstimate:
		p, ok := paramsAs[SendEstimateParams](params)
		if !ok {
			return badParams(params)
		}
		return validSendSpec(p.Spec)
	case MethodSend:
		p, ok := paramsAs[SendParams](params)
		if !ok {
			return badParams(params)
		}
		return validSendSpec(p.Spec)
	case MethodReceive:
		p, ok := paramsAs[ReceiveParams](params)
		if !ok {
			return badParams(params)
		}
		return validRecvSpec(p.Spec)
	case MethodReceiveBusy:
		p, ok := paramsAs[ReceiveBusyParams](params)
		if !ok {
			return badParams(params)
		}
		for _, ds := range p.Datasets {
			if !validDatasetName(ds) {
				return &InvalidError{Field: "datasets", Value: ds}
			}
		}
	case MethodInfo, MethodShutdown, MethodCancel:
		// Nothing from the wire reaches a command line.
	default:
		return &InvalidError{Field: "params", Value: method}
	}
	return nil
}

// paramsAs returns params as a T, accepting T or a non-nil *T.
func paramsAs[T any](params any) (T, bool) {
	switch p := params.(type) {
	case T:
		return p, true
	case *T:
		if p != nil {
			return *p, true
		}
	}
	var zero T
	return zero, false
}

func badParams(params any) error {
	return &InvalidError{Field: "params", Value: fmt.Sprintf("%T", params)}
}

func validDataset(field, ds string) error {
	if !validDatasetName(ds) {
		return &InvalidError{Field: field, Value: ds}
	}
	return nil
}

// validDatasetName reports whether ds is a "/"-joined list of valid
// components of at most 255 bytes that does not start with "-".
func validDatasetName(ds string) bool {
	if len(ds) > 255 || strings.HasPrefix(ds, "-") {
		return false
	}
	for _, c := range strings.Split(ds, "/") {
		if !componentRE.MatchString(c) {
			return false
		}
	}
	return true
}

// validShort reports whether s is a valid snapshot or bookmark short name:
// one component not starting with "-".
func validShort(s string) bool {
	return !strings.HasPrefix(s, "-") && componentRE.MatchString(s)
}

// validJoined reports whether name is "dataset<sep>short", the form of a
// full snapshot (sep '@') or bookmark (sep '#') name.
func validJoined(name string, sep byte) bool {
	ds, short, ok := strings.Cut(name, string(sep))
	return ok && validDatasetName(ds) && validShort(short)
}

// validTag reports whether tag is a valid hold tag: 1 to 255 bytes, not
// starting with "-", without control characters.
func validTag(tag string) bool {
	if len(tag) == 0 || len(tag) > 255 || tag[0] == '-' {
		return false
	}
	for i := 0; i < len(tag); i++ {
		if tag[i] < 0x20 || tag[i] == 0x7f {
			return false
		}
	}
	return true
}

func validSendSpec(s zfs.SendSpec) error {
	if err := validDataset("spec.dataset", s.Dataset); err != nil {
		return err
	}
	for _, f := range []struct {
		field, value string
		valid        func(string) bool
	}{
		{"spec.to", s.To, validShort},
		{"spec.from_snap", s.FromSnap, validShort},
		{"spec.from_bookmark", s.FromBookmark, validShort},
		{"spec.from_origin", s.FromOrigin, func(v string) bool { return validJoined(v, '@') }},
		{"spec.resume_token", s.ResumeToken, resumeTokenRE.MatchString},
	} {
		if f.value != "" && !f.valid(f.value) {
			return &InvalidError{Field: f.field, Value: f.value}
		}
	}
	for _, o := range s.Options {
		if strings.IndexByte(zfs.SendAllowedStream, o.Flag) < 0 || o.Value != "" {
			return &InvalidError{Field: "spec.options", Value: string(o.Flag) + o.Value}
		}
	}
	return nil
}

func validRecvSpec(s zfs.RecvSpec) error {
	if err := validDataset("spec.dataset", s.Dataset); err != nil {
		return err
	}
	for i := 0; i < len(s.Args); i++ {
		a := s.Args[i]
		switch {
		case recvFlags[a]:
		case a == "-x":
			if i+1 >= len(s.Args) || !propNameRE.MatchString(s.Args[i+1]) {
				return recvArgsError(s.Args, i+1, a)
			}
			i++
		case a == "-o":
			if i+1 >= len(s.Args) || !validPropAssignment(s.Args[i+1]) {
				return recvArgsError(s.Args, i+1, a)
			}
			i++
		default:
			return &InvalidError{Field: "spec.args", Value: a}
		}
	}
	return nil
}

// recvArgsError reports Args[i] as the bad value, or flag when it has no
// value following it.
func recvArgsError(args []string, i int, flag string) error {
	if i < len(args) {
		return &InvalidError{Field: "spec.args", Value: args[i]}
	}
	return &InvalidError{Field: "spec.args", Value: flag}
}

// validPropAssignment reports whether s is "prop=value" with a valid
// property name and a value free of NUL and newline.
func validPropAssignment(s string) bool {
	prop, value, ok := strings.Cut(s, "=")
	return ok && propNameRE.MatchString(prop) && !strings.ContainsAny(value, "\x00\n")
}
