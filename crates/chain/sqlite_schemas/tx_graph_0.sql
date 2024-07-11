-- schema version control
CREATE TABLE bdk_tx_graph_version ( version INTEGER ) STRICT;
INSERT INTO bdk_tx_graph_version VALUES (0);

-- txid is transaction hash hex string (reversed)
-- whole_tx is a consensus encoded transaction,
-- last seen is a u64 unix epoch seconds
CREATE TABLE bdk_tx_graph_txs
(
    txid      TEXT PRIMARY KEY NOT NULL,
    whole_tx  BLOB,
    last_seen INTEGER
) STRICT;

-- Outpoint txid hash hex string (reversed)
-- Outpoint vout
-- TxOut value as SATs
-- TxOut script consensus encoded
CREATE TABLE bdk_tx_graph_txouts
(
    txid   TEXT    NOT NULL,
    vout   INTEGER NOT NULL,
    value  INTEGER NOT NULL,
    script BLOB    NOT NULL,
    PRIMARY KEY (txid, vout)
) STRICT;

-- join table between anchor and tx
-- anchor is a json serialized Anchor structure as JSONB,
-- txid is transaction hash hex string (reversed)
-- block hash hex string
-- block height is a u32
CREATE TABLE bdk_tx_graph_anchors
(
    anchor              BLOB NOT NULL,
    txid                TEXT NOT NULL REFERENCES bdk_tx_graph_txs (txid),
    block_hash          TEXT NOT NULL,
    block_height        INTEGER NOT NULL,
    UNIQUE (anchor, txid)
) STRICT;
