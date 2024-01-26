package objcli

import "io"

// CopyReadSeeker copies from src to dst, seeking back to the current position in src upon completion.
//
// NOTE: Volatile API that's subject to change/removal.
func CopyReadSeeker(dst io.Writer, src io.ReadSeeker) (int64, error) {
	cur, err := src.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}

	n, err := io.Copy(dst, src)
	if err != nil {
		return n, err
	}

	_, err = src.Seek(cur, io.SeekStart)
	if err != nil {
		return n, err
	}

	return n, nil
}

// SeekerLength calculates the number of bytes in the given seeker.
//
// NOTE: Volatile API that's subject to change/removal.
func SeekerLength(seeker io.Seeker) (int64, error) {
	cur, err := seeker.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}

	end, err := seeker.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	_, err = seeker.Seek(cur, io.SeekStart)
	if err != nil {
		return 0, err
	}

	return end - cur, nil
}
