-- Old sensor data (several months ago, spanning multiple chunks)
INSERT INTO sensor_data (time, sensor_id, temperature, humidity, location) VALUES
    ('2023-01-15 10:00:00+00', 1, 22.5, 45.0, 'warehouse-A'),
    ('2023-01-15 10:05:00+00', 2, 23.1, 44.5, 'warehouse-B'),
    ('2023-02-20 11:30:00+00', 1, 21.0, 50.0, 'warehouse-A'),
    ('2023-02-20 11:35:00+00', 3, 19.8, 55.0, 'warehouse-C'),
    ('2023-03-10 09:15:00+00', 2, 24.0, 42.0, 'warehouse-B'),
    ('2023-04-05 14:00:00+00', 1, 25.5, 38.0, 'warehouse-A'),
    ('2023-05-25 16:45:00+00', 3, 20.0, 48.0, 'warehouse-C');

-- Recent sensor data (should NOT be archived)
INSERT INTO sensor_data (time, sensor_id, temperature, humidity, location) VALUES
    (NOW() - INTERVAL '1 day',  1, 22.0, 46.0, 'warehouse-A'),
    (NOW() - INTERVAL '2 days', 2, 23.5, 43.0, 'warehouse-B'),
    (NOW() - INTERVAL '5 days', 3, 21.5, 49.0, 'warehouse-C');

-- Old events (regular table)
INSERT INTO events (event_type, payload, created_at) VALUES
    ('login',  '{"user": "alice"}', '2023-01-15 10:00:00'),
    ('logout', '{"user": "alice"}', '2023-01-15 18:00:00'),
    ('login',  '{"user": "bob"}',   '2023-02-20 11:30:00'),
    ('error',  NULL,                '2023-03-10 09:15:00'),
    ('login',  '{"user": "charlie"}', '2023-04-05 14:00:00');

-- Recent events
INSERT INTO events (event_type, payload, created_at) VALUES
    ('login',  '{"user": "diana"}', NOW() - INTERVAL '1 day'),
    ('logout', '{"user": "diana"}', NOW() - INTERVAL '1 day'),
    ('login',  '{"user": "eve"}',   NOW() - INTERVAL '3 days');
