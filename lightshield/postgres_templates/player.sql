CREATE TABLE match_data.player
(
    -- Central
    "created_at"                               TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    "gameId"                                   bigint,
    "platformId"                               VARCHAR,
    "gameDuration"                             int,
    "gameStartTimestamp"                       TIMESTAMP WITHOUT TIME ZONE,
    "gameCreation"                             TIMESTAMP WITHOUT TIME ZONE,
    "patch"                                    REAL,
    "mapId"                                    smallint,
    "queueId"                                  smallint,
    win                                        boolean,
    "gameEndedInEarlySurrender"                boolean,
    "gameEndedInSurrender"                     boolean,
    -- Player meta
    "pickTurn"                                 smallint, -- Encodes team
    "ban"                                      smallint,
    "assists"                                  smallint,
    "puuid"                                    VARCHAR(78),
    "individualPosition"                       VARCHAR(1),
    "teamPosition"                             VARCHAR(1),
    -- Basics
    "championId"                               REAL,     -- Encodes transformation
    "experience"                               smallint, -- Encodes level
    "bountyLevel"                              smallint,
    "baronKills"                               smallint,
    "kills"                                    smallint,
    "deaths"                                   smallint,
    -- Misc
    "longestTimeSpentLiving"                   smallint,
    "teamEarlySurrendered"                     boolean,
    "totalTimeSpentDead"                       smallint,
    "largestCriticalStrike"                    smallint,
    "timeCCingOthers"                          smallint,
    "totalTimeCCDealt"                         smallint,
    -- -kills/-assists/-takedowns
    "firstBloodAssist"                         boolean,
    "firstBloodKill"                           boolean,
    "doubleKills"                              smallint,
    "tripleKills"                              smallint,
    "quadraKills"                              smallint,
    "pentaKills"                               smallint,
    "unrealKills"                              smallint,
    "killingSprees"                            smallint,
    "largestMultiKill"                         smallint,
    "largestKillingSpree"                      smallint,
    -- Farming
    "goldEarned"                               integer,
    "goldSpent"                                integer,
    "neutralMinionsKilled"                     smallint,
    "totalMinionsKilled"                       smallint,
    -- Objectives
    "dragonKills"                              smallint,
    "firstTowerAssist"                         boolean,
    "firstTowerKill"                           boolean,
    "inhibitorKills"                           smallint,
    "inhibitorTakedowns"                       smallint,
    "inhibitorsLost"                           smallint,
    "objectivesStolen"                         smallint,
    "objectivesStolenAssists"                  smallint,
    "turretKills"                              smallint,
    "turretTakedowns"                          smallint,
    "turretsLost"                              smallint,
    -- Items
    "item0"                                    smallint,
    "item1"                                    smallint,
    "item2"                                    smallint,
    "item3"                                    smallint,
    "item4"                                    smallint,
    "item5"                                    smallint,
    "item6"                                    smallint,
    "consumablesPurchased"                     smallint,
    "itemsPurchased"                           smallint,
    -- Perks
    "statPerks"                                smallint, -- Encodes all 3
    "primaryStyle"                             smallint, -- Encodes all positions
    "subStyle"                                 smallint, -- Encodes all positions
    -- Wards
    "visionScore"                              smallint,
    "visionWardsBoughtInGame"                  smallint,
    "wardsKilled"                              smallint,
    "wardsPlaced"                              smallint,
    "detectorWardsPlaced"                      smallint,
    -- Spells
    "spell1Casts"                              smallint,
    "spell2Casts"                              smallint,
    "spell3Casts"                              smallint,
    "spell4Casts"                              smallint,
    "summoner1Casts"                           smallint,
    "summoner1Id"                              smallint,
    "summoner2Casts"                           smallint,
    "summoner2Id"                              smallint,
    -- Damage Numbers
    "damageDealtToBuildings"                   integer,
    "damageDealtToObjectives"                  integer,
    "damageDealtToTurrets"                     integer,
    "damageSelfMitigated"                      integer,
    "magicDamageDealt"                         integer,
    "magicDamageDealtToChampions"              integer,
    "magicDamageTaken"                         integer,
    "physicalDamageDealt"                      integer,
    "physicalDamageDealtToChampions"           integer,
    "physicalDamageTaken"                      integer,
    "totalDamageDealt"                         integer,
    "totalDamageDealtToChampions"              integer,
    "totalDamageShieldedOnTeammates"           integer,
    "totalDamageTaken"                         integer,
    "totalHeal"                                integer,
    "totalHealsOnTeammates"                    integer,
    "trueDamageDealt"                          integer,
    "trueDamageDealtToChampions"               integer,
    "trueDamageTaken"                          integer,
    -- Challenges
    "12AssistStreakCount"                      SMALLINT,
    "abilityUses"                              SMALLINT,
    "acesBefore15Minutes"                      SMALLINT,
    "alliedJungleMonsterKills"                 SMALLINT,
    "baronBuffGoldAdvantageOverThreshold"      SMALLINT,
    "baronTakedowns"                           SMALLINT,
    "blastConeOppositeOpponentCount"           SMALLINT,
    "bountyGold"                               SMALLINT,
    "buffsStolen"                              SMALLINT,
    "completeSupportQuestInTime"               SMALLINT,
    "controlWardsPlaced"                       SMALLINT,
    "damagePerMinute"                          REAL,
    "damageTakenOnTeamPercentage"              REAL,
    "dancedWithRiftHerald"                     SMALLINT,
    "deathsByEnemyChamps"                      SMALLINT,
    "dodgeSkillShotsSmallWindow"               SMALLINT,
    "doubleAces"                               SMALLINT,
    "dragonTakedowns"                          SMALLINT,
    "earliestBaron"                            REAL,
    "earlyLaningPhaseGoldExpAdvantage"         SMALLINT,
    "effectiveHealAndShielding"                REAL,
    "elderDragonKillsWithOpposingSoul"         SMALLINT,
    "elderDragonMultikills"                    SMALLINT,
    "enemyChampionImmobilizations"             SMALLINT,
    "enemyJungleMonsterKills"                  REAL,
    "epicMonsterKillsNearEnemyJungler"         SMALLINT,
    "epicMonsterKillsWithin30SecondsOfSpawn"   SMALLINT,
    "epicMonsterSteals"                        SMALLINT,
    "epicMonsterStolenWithoutSmite"            SMALLINT,
    "fastestLegendary"                         REAL,
    "firstTurretKilledTime"                    REAL,
    "flawlessAces"                             SMALLINT,
    "fullTeamTakedown"                         SMALLINT,
    "gameLength"                               REAL,
    "getTakedownsInAllLanesEarlyJungleAsLaner" SMALLINT,
    "goldPerMinute"                            REAL,
    "hadAfkTeammate"                           SMALLINT,
    "hadOpenNexus"                             SMALLINT,
    "immobilizeAndKillWithAlly"                SMALLINT,
    "initialBuffCount"                         SMALLINT,
    "initialCrabCount"                         SMALLINT,
    "jungleCsBefore10Minutes"                  SMALLINT,
    "junglerTakedownsNearDamagedEpicMonster"   SMALLINT,
    "kTurretsDestroyedBeforePlatesFall"        SMALLINT,
    "kda"                                      REAL,
    "killAfterHiddenWithAlly"                  SMALLINT,
    "killParticipation"                        REAL,
    "killedChampTookFullTeamDamageSurvived"    SMALLINT,
    "killsNearEnemyTurret"                     SMALLINT,
    "killsOnOtherLanesEarlyJungleAsLaner"      SMALLINT,
    "killsOnRecentlyHealedByAramPack"          SMALLINT,
    "killsUnderOwnTurret"                      SMALLINT,
    "killsWithHelpFromEpicMonster"             SMALLINT,
    "knockEnemyIntoTeamAndKill"                SMALLINT,
    "landSkillShotsEarlyGame"                  SMALLINT,
    "laneMinionsFirst10Minutes"                SMALLINT,
    "laningPhaseGoldExpAdvantage"              SMALLINT,
    "legendaryCount"                           SMALLINT,
    "lostAnInhibitor"                          SMALLINT,
    "maxCsAdvantageOnLaneOpponent"             SMALLINT,
    "maxKillDeficit"                           SMALLINT,
    "maxLevelLeadLaneOpponent"                 SMALLINT,
    "moreEnemyJungleThanOpponent"              REAL,
    "multiKillOneSpell"                        SMALLINT,
    "multiTurretRiftHeraldCount"               SMALLINT,
    "multikills"                               SMALLINT,
    "multikillsAfterAggressiveFlash"           SMALLINT,
    "mythicItemUsed"                           SMALLINT,
    "outerTurretExecutesBefore10Minutes"       SMALLINT,
    "outnumberedKills"                         SMALLINT,
    "outnumberedNexusKill"                     SMALLINT,
    "perfectDragonSoulsTaken"                  SMALLINT,
    "perfectGame"                              SMALLINT,
    "pickKillWithAlly"                         SMALLINT,
    "poroExplosions"                           SMALLINT,
    "quickCleanse"                             SMALLINT,
    "quickFirstTurret"                         SMALLINT,
    "quickSoloKills"                           SMALLINT,
    "riftHeraldTakedowns"                      SMALLINT,
    "saveAllyFromDeath"                        SMALLINT,
    "scuttleCrabKills"                         SMALLINT,
    "shortestTimeToAceFromFirstTakedown"       REAL,
    "skillshotsDodged"                         SMALLINT,
    "skillshotsHit"                            SMALLINT,
    "snowballsHit"                             SMALLINT,
    "soloBaronKills"                           SMALLINT,
    "soloKills"                                SMALLINT,
    "soloTurretsLategame"                      SMALLINT,
    "stealthWardsPlaced"                       SMALLINT,
    "survivedSingleDigitHpCount"               SMALLINT,
    "survivedThreeImmobilizesInFight"          SMALLINT,
    "takedownOnFirstTurret"                    SMALLINT,
    "takedowns"                                SMALLINT,
    "takedownsAfterGainingLevelAdvantage"      SMALLINT,
    "takedownsBeforeJungleMinionSpawn"         SMALLINT,
    "takedownsFirstXMinutes"                   SMALLINT,
    "takedownsInAlcove"                        SMALLINT,
    "takedownsInEnemyFountain"                 SMALLINT,
    "teamBaronKills"                           SMALLINT,
    "teamDamagePercentage"                     REAL,
    "teamElderDragonKills"                     SMALLINT,
    "teamRiftHeraldKills"                      SMALLINT,
    "thirdInhibitorDestroyedTime"              REAL,
    "threeWardsOneSweeperCount"                SMALLINT,
    "tookLargeDamageSurvived"                  SMALLINT,
    "turretPlatesTaken"                        SMALLINT,
    "turretsTakenWithRiftHerald"               SMALLINT,
    "twentyMinionsIn3SecondsCount"             SMALLINT,
    "unseenRecalls"                            SMALLINT,
    "visionScoreAdvantageLaneOpponent"         REAL,
    "visionScorePerMinute"                     REAL,
    "wardTakedowns"                            SMALLINT,
    "wardTakedownsBefore20M"                   SMALLINT,
    "wardsGuarded"                             SMALLINT,
    PRIMARY KEY ("gameId", "platformId", "puuid", "patch")
) PARTITION BY LIST ("platformId");