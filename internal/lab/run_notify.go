package lab

// NotifyRunClosed participates in the same fault boundary as queue notifications.
func (tx *faultTx) NotifyRunClosed(runID string) {
	tx.base.NotifyRunClosed(runID)
	if err := tx.after(nil); err != nil {
		panic(err)
	}
}
