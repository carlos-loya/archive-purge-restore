-- Old rows (hardcoded 2023 dates, well past any 30-day days_online)
INSERT INTO orders (id, customer, amount, notes, shipped, created_at) VALUES
    (1, 'Alice',   99.99,  'Express shipping', TRUE,  '2023-01-15 10:00:00'),
    (2, 'Bob',     NULL,   NULL,               FALSE, '2023-02-20 11:30:00'),
    (3, 'Charlie', 250.00, 'Gift wrap',        TRUE,  '2023-03-10 09:15:00'),
    (4, 'Diana',   15.50,  NULL,               FALSE, '2023-04-05 14:00:00'),
    (5, 'Eve',     0.00,   'Free sample',      TRUE,  '2023-05-25 16:45:00');

-- Recent rows (will survive archival)
INSERT INTO orders (id, customer, amount, notes, shipped, created_at) VALUES
    (6, 'Frank',   120.00, 'Priority',    TRUE,  NOW() - INTERVAL '1 day'),
    (7, 'Grace',   45.99,  NULL,          FALSE, NOW() - INTERVAL '2 days'),
    (8, 'Heidi',   300.00, 'Bulk order',  TRUE,  NOW() - INTERVAL '5 days');

-- Old order_items
INSERT INTO order_items (order_id, item_id, product, quantity, created_at) VALUES
    (1, 1, 'Widget A', 2,  '2023-01-15 10:00:00'),
    (1, 2, 'Widget B', 1,  '2023-01-15 10:00:00'),
    (2, 1, 'Gadget X', 5,  '2023-02-20 11:30:00'),
    (3, 1, 'Part Y',   10, '2023-03-10 09:15:00'),
    (4, 1, 'Part Z',   3,  '2023-04-05 14:00:00');

-- Recent order_items
INSERT INTO order_items (order_id, item_id, product, quantity, created_at) VALUES
    (6, 1, 'New Widget', 1, NOW() - INTERVAL '1 day'),
    (7, 1, 'New Gadget', 2, NOW() - INTERVAL '2 days'),
    (8, 1, 'Bulk Item',  50, NOW() - INTERVAL '5 days');
