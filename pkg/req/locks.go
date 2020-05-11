package req

func lockTreeDelayed(qid string) string {
	return "req_lock_tree_delayed" + qid
}

func lockTakenValidation(qid string) string {
	return "req_lock_taken_validation" + qid
}

func lockKickAllInProgress(qid string) string {
	return "req_lock_kickall" + qid
}