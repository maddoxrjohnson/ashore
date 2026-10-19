package native

// harden is what the child does to itself after the pivot and before the
// exec. Step 4.5 fills it in: capabilities, no_new_privs, and the uid.
func harden(initSpec) error { return nil }
