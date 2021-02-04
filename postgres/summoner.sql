CREATE TABLE IF NOT EXISTS summoner (

                                        summoner_id         VARCHAR(63) PRIMARY KEY,
                                        account_id          VARCHAR(56),
                                        puuid               VARCHAR(78),

                                        rank                SMALLINT,
                                        rank_history        SMALLINT[],

                                        wins                SMALLINT,
                                        wins_last_updated   SMALLINT,

                                        losses              SMALLINT DEFAULT 0,
                                        losses_last_updated SMALLINT DEFAULT 0,

                                        priority            VARCHAR(1)
                                            checked_out TIMESTAMP
);


