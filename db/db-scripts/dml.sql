-- Inserindo dados na tabela Accounts
INSERT INTO accounts (account_id, email, password, registration_date)
VALUES
(1, 'player1@example.com', 'password123', '2024-01-01'),
(2, 'player2@example.com', 'password123', '2024-01-02'),
(3, 'player3@example.com', 'password123', '2024-01-03'),
(4, 'player4@example.com', 'password123', '2024-01-04'),
(5, 'player5@example.com', 'password123', '2024-01-05'),
(6, 'player6@example.com', 'password123', '2024-01-06'),
(7, 'player7@example.com', 'password123', '2024-01-07'),
(8, 'player8@example.com', 'password123', '2024-01-08'),
(9, 'player9@example.com', 'password123', '2024-01-09'),
(10, 'player10@example.com', 'password123', '2024-01-10');

-- Inserindo dados na tabela Players
INSERT INTO players (player_id, account_id, player_name, level, guild_id)
VALUES
(1, 1, 'Player1', 10, 1),
(2, 2, 'Player2', 20, 2),
(3, 3, 'Player3', 15, 3),
(4, 4, 'Player4', 25, 4),
(5, 5, 'Player5', 30, 5),
(6, 6, 'Player6', 35, 6),
(7, 7, 'Player7', 40, 7),
(8, 8, 'Player8', 45, 8),
(9, 9, 'Player9', 50, 9),
(10, 10, 'Player10', 55, 10);

-- Inserindo dados na tabela Guilds
INSERT INTO guilds (guild_id, guild_name, guild_leader)
VALUES
(1, 'Guild1', 1),
(2, 'Guild2', 2),
(3, 'Guild3', 3),
(4, 'Guild4', 4),
(5, 'Guild5', 5),
(6, 'Guild6', 6),
(7, 'Guild7', 7),
(8, 'Guild8', 8),
(9, 'Guild9', 9),
(10, 'Guild10', 10);

-- Inserindo dados na tabela Achievements
INSERT INTO achievements (achievement_id, player_id, achievement_name, date_achieved)
VALUES
(1, 1, 'First Kill', '2024-01-01'),
(2, 2, 'Master Miner', '2024-01-02'),
(3, 3, 'Champion', '2024-01-03'),
(4, 4, 'Explorer', '2024-01-04'),
(5, 5, 'Treasure Hunter', '2024-01-05'),
(6, 6, 'PvP Winner', '2024-01-06'),
(7, 7, 'Dungeon Master', '2024-01-07'),
(8, 8, 'Crafting Expert', '2024-01-08'),
(9, 9, 'Quest Champion', '2024-01-09'),
(10, 10, 'Event Winner', '2024-01-10');

-- Inserindo dados na tabela Transactions
INSERT INTO transactions (transaction_id, account_id, transaction_type, amount, transaction_date)
VALUES
(1, 1, 'Purchase', 100.00, '2024-01-01'),
(2, 2, 'Purchase', 150.00, '2024-01-02'),
(3, 3, 'Purchase', 200.00, '2024-01-03'),
(4, 4, 'Purchase', 250.00, '2024-01-04'),
(5, 5, 'Purchase', 300.00, '2024-01-05'),
(6, 6, 'Purchase', 350.00, '2024-01-06'),
(7, 7, 'Purchase', 400.00, '2024-01-07'),
(8, 8, 'Purchase', 450.00, '2024-01-08'),
(9, 9, 'Purchase', 500.00, '2024-01-09'),
(10, 10, 'Purchase', 550.00, '2024-01-10');

-- Inserindo dados na tabela Items
INSERT INTO items (item_id, player_id, item_name, item_type, date_received)
VALUES
(1, 1, 'Sword', 'Weapon', '2024-01-01'),
(2, 2, 'Shield', 'Armor', '2024-01-02'),
(3, 3, 'Potion', 'Consumable', '2024-01-03'),
(4, 4, 'Helmet', 'Armor', '2024-01-04'),
(5, 5, 'Bow', 'Weapon', '2024-01-05'),
(6, 6, 'Axe', 'Weapon', '2024-01-06'),
(7, 7, 'Armor', 'Armor', '2024-01-07'),
(8, 8, 'Ring', 'Accessory', '2024-01-08'),
(9, 9, 'Boots', 'Armor', '2024-01-09'),
(10, 10, 'Gloves', 'Armor', '2024-01-10');
