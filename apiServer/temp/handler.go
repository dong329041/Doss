package temp

import "net/http"

func Handler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodHead {
		head(w, r)
		return
	}
	if r.Method == http.MethodPut {
		put(w, r)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}
