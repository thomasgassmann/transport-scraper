def bellman_ford(from_station, connections, weight):
    distances = dict()
    parent = dict()
    via = dict()
    distances[from_station] = 0
    done = False
    iterations = 0
    while not done:
        done = True
        for conn in connections:
            res_exp = conn.to_station_id in distances
            src_exp = conn.from_station_id in distances
            if (not res_exp and src_exp) or (res_exp and src_exp and distances[conn.to_station_id] > distances[conn.from_station_id] + weight(conn)):
                distances[conn.to_station_id] = distances[conn.from_station_id] + weight(conn)
                parent[conn.to_station_id] = conn.from_station_id
                via[conn.to_station_id] = conn

                done = False
        iterations += 1
    return (distances, parent, via, iterations)
