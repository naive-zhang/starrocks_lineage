package com.starrocks.lineage.pipeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * User name → dense, collision-free BIGINT id for {@code user_bitmap} (review R-04 of Stage 1: StarRocks 3.1 has no
 * {@code bitmap_hash64} and 32-bit {@code bitmap_hash} collides, which would make UV inexact).
 *
 * <p>The pipeline is the single writer of {@code audit_user_dict}: the dictionary is loaded at the start of every
 * cycle, new users get {@code max(id) + 1, + 2, ...}, and the new entries are persisted <b>before</b> the usage rows
 * that reference them. Re-running a failed cycle reloads the persisted ids, so an id is never reassigned.</p>
 * Thread-safe.
 */
public final class UserDictionary {

    private final Map<String, Long> ids = new HashMap<>();
    private final List<Entry> pending = new ArrayList<>();
    private long maxId;

    /**
     * @param persisted ids already stored in {@code audit_user_dict}
     * @throws IllegalArgumentException when two users share an id or an id is not positive
     */
    public UserDictionary(Map<String, Long> persisted) {
        Map<Long, String> reverse = new HashMap<>();
        for (Map.Entry<String, Long> e : persisted.entrySet()) {
            long id = Objects.requireNonNull(e.getValue(), "id");
            if (id <= 0) {
                throw new IllegalArgumentException("user id must be > 0: " + e);
            }
            String previous = reverse.put(id, e.getKey());
            if (previous != null) {
                throw new IllegalArgumentException("id " + id + " assigned to both " + previous + " and "
                        + e.getKey());
            }
            ids.put(e.getKey(), id);
            maxId = Math.max(maxId, id);
        }
    }

    /** @return the id of {@code user}, assigning the next one when the user is new */
    public synchronized long idOf(String user) {
        Objects.requireNonNull(user, "user");
        Long id = ids.get(user);
        if (id != null) {
            return id;
        }
        long assigned = ++maxId;
        ids.put(user, assigned);
        pending.add(new Entry(user, assigned));
        return assigned;
    }

    /** @return entries assigned since the last call, to be persisted */
    public synchronized List<Entry> drainPending() {
        List<Entry> result = new ArrayList<>(pending);
        pending.clear();
        return Collections.unmodifiableList(result);
    }

    public synchronized int size() {
        return ids.size();
    }

    /**
     * A new dictionary entry.
     */
    public static final class Entry {
        private final String userName;
        private final long userId;

        Entry(String userName, long userId) {
            this.userName = userName;
            this.userId = userId;
        }

        public String getUserName() {
            return userName;
        }

        public long getUserId() {
            return userId;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Entry)) {
                return false;
            }
            Entry e = (Entry) o;
            return userId == e.userId && userName.equals(e.userName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(userName, userId);
        }

        @Override
        public String toString() {
            return userName + "=" + userId;
        }
    }
}
