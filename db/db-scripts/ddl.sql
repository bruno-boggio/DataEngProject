-- Tabela Accounts
CREATE TABLE accounts (
    account_id INT PRIMARY KEY,
    email VARCHAR(255),
    password VARCHAR(255),
    registration_date DATE
);

-- Tabela Players (Agora criada primeiro)
CREATE TABLE players (
    player_id INT PRIMARY KEY,
    account_id INT,
    player_name VARCHAR(255),
    level INT,
    guild_id INT,
    FOREIGN KEY (account_id) REFERENCES accounts(account_id)
);

-- Tabela Guilds (Agora criada depois de players)
CREATE TABLE guilds (
    guild_id INT PRIMARY KEY,
    guild_name VARCHAR(255),
    guild_leader INT,
    FOREIGN KEY (guild_leader) REFERENCES players(player_id) -- Agora players já existe
);

-- Tabela Achievements
CREATE TABLE achievements (
    achievement_id INT PRIMARY KEY,
    player_id INT,
    achievement_name VARCHAR(255),
    date_achieved DATE,
    FOREIGN KEY (player_id) REFERENCES players(player_id)
);

-- Tabela Transactions
CREATE TABLE transactions (
    transaction_id INT PRIMARY KEY,
    account_id INT,
    transaction_type VARCHAR(50),
    amount DECIMAL(10,2),
    transaction_date DATE,
    FOREIGN KEY (account_id) REFERENCES accounts(account_id)
);

-- Tabela Items
CREATE TABLE items (
    item_id INT PRIMARY KEY,
    player_id INT,
    item_name VARCHAR(255),
    item_type VARCHAR(50),
    date_received DATE,
    FOREIGN KEY (player_id) REFERENCES players(player_id)
);
