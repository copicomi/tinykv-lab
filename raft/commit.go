package raft

func (rf *Raft) UpdateCommitIndex() {
	l := rf.RaftLog.committed
	r := rf.RaftLog.LastIndex()
	for l < r {
		mid := (l + r + 1) / 2
		if rf.IsReadyToCommit(mid) {
			l = mid
		} else {
			r = mid - 1
		}
	}
	if l == rf.RaftLog.committed {
		return
	}
	commit_term, err := rf.RaftLog.Term(l)
	if err != nil {
		panic(err)
	}
	if commit_term == rf.Term {
		rf.RaftLog.committed = l
		rf.bcastAppend()
		rf.bcastApply()
	}
}

func (r *Raft) IsReadyToCommit(index uint64) bool {
	count := 1
	for _, i := range r.peers {
		if i == r.id {
			continue
		}
		if r.Prs[i].Match >= index {
			count++
		}
	}
	return count > len(r.peers)/2
}
