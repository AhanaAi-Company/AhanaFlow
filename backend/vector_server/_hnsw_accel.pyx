# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

import heapq


def search_layer_accel(
    query,
    int entry_point,
    int ef,
    int layer,
    matrix,
    norms,
    nodes,
    distance_single,
    distance_batch,
):
    cdef double ep_dist = float(distance_single(query, entry_point, matrix, norms))
    cdef list candidates = [(ep_dist, entry_point)]
    cdef set visited = {entry_point}
    cdef list results = [(-ep_dist, entry_point)]
    cdef object c_node
    cdef object n_node
    cdef object distances
    cdef list neighbor_indices
    cdef Py_ssize_t i
    cdef int c_idx
    cdef int n_idx
    cdef double c_dist
    cdef double d
    cdef double worst_dist

    while candidates:
        c_dist, c_idx = heapq.heappop(candidates)

        if results and c_dist > -results[0][0] and len(results) >= ef:
            break

        c_node = nodes.get(c_idx)
        if c_node is None or c_node.deleted:
            continue

        neighbor_indices = []
        if layer < len(c_node.neighbors):
            for n_idx in c_node.neighbors[layer]:
                if n_idx in visited:
                    continue
                visited.add(n_idx)
                n_node = nodes.get(n_idx)
                if n_node is not None and not n_node.deleted:
                    neighbor_indices.append(n_idx)

        if not neighbor_indices:
            continue

        distances = distance_batch(query, neighbor_indices, matrix, norms)
        for i in range(len(neighbor_indices)):
            n_idx = neighbor_indices[i]
            d = float(distances[i])
            worst_dist = -results[0][0] if results else float("inf")
            if len(results) < ef or d < worst_dist:
                heapq.heappush(candidates, (d, n_idx))
                heapq.heappush(results, (-d, n_idx))
                if len(results) > ef:
                    heapq.heappop(results)

    return sorted(((idx, -neg_dist) for neg_dist, idx in results), key=lambda x: x[1])