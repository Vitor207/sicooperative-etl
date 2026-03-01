-- =========================
-- TABELA: associado
-- =========================

CREATE TABLE associado (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    sobrenome VARCHAR(100) NOT NULL,
    data_nascimento DATE NOT NULL,
	idade int NOT NULL,
    email VARCHAR(150) UNIQUE NOT NULL
);

-- =========================
-- TABELA: conta
-- =========================

CREATE TABLE conta (
    id SERIAL PRIMARY KEY,
    tipo VARCHAR(20) NOT NULL,
    data_criacao TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    id_associado INT NOT NULL,
    
    CONSTRAINT fk_conta_associado
        FOREIGN KEY (id_associado)
        REFERENCES associado(id)
        ON DELETE CASCADE
);

-- =========================
-- TABELA: cartao
-- =========================
CREATE TABLE cartao (
    id SERIAL PRIMARY KEY,
    num_cartao VARCHAR(16) NOT NULL UNIQUE,
    nom_impresso VARCHAR(100) NOT NULL,
	data_criacao_cartao TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    id_conta INT NOT NULL,
    
    CONSTRAINT fk_cartao_conta
        FOREIGN KEY (id_conta)
        REFERENCES conta(id)
        ON DELETE CASCADE
);

-- =========================
-- TABELA: movimento
-- =========================
CREATE TABLE movimento (
    id SERIAL PRIMARY KEY,
    vlr_transacao DECIMAL(10,2) NOT NULL,
    des_transacao VARCHAR(255),
    data_movimento TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    id_cartao INT NOT NULL,
    
    CONSTRAINT fk_movimento_cartao
        FOREIGN KEY (id_cartao)
        REFERENCES cartao(id)
        ON DELETE CASCADE
);