package req

func keyListReady(qid string) string {
	return "req_list_ready" + qid
}

func keyListTaken(qid string) string {
	return "req_list_taken" + qid
}

func keyTreeDelayed(qid string) string {
	return "req_tree_delayed" + qid
}

func keySetBuried(qid string) string {
	return "req_set_buried" + qid
}

func keyQName(qid string) string {
	return "req_qname" + qid
}

func keyLastValidationTs(qid string) string {
	return "req_last_validation" + qid
}

func keyCounterDone(qid string) string {
	return "req_count_done" + qid
}
