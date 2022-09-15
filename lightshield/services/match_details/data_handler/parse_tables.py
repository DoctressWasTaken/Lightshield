from hashlib import blake2b
from datetime import datetime

challenges_420 = [
    "12AssistStreakCount",
    "abilityUses",
    "acesBefore15Minutes",
    "alliedJungleMonsterKills",
    "baronBuffGoldAdvantageOverThreshold",
    "baronTakedowns",
    "blastConeOppositeOpponentCount",
    "bountyGold",
    "buffsStolen",
    "completeSupportQuestInTime",
    "controlWardsPlaced",
    "damagePerMinute",
    "damageTakenOnTeamPercentage",
    "dancedWithRiftHerald",
    "deathsByEnemyChamps",
    "dodgeSkillShotsSmallWindow",
    "doubleAces",
    "dragonTakedowns",
    "earliestBaron",
    "earlyLaningPhaseGoldExpAdvantage",
    "effectiveHealAndShielding",
    "elderDragonKillsWithOpposingSoul",
    "elderDragonMultikills",
    "enemyChampionImmobilizations",
    "enemyJungleMonsterKills",
    "epicMonsterKillsNearEnemyJungler",
    "epicMonsterKillsWithin30SecondsOfSpawn",
    "epicMonsterSteals",
    "epicMonsterStolenWithoutSmite",
    "fastestLegendary",
    "firstTurretKilledTime",
    "flawlessAces",
    "fullTeamTakedown",
    "gameLength",
    "getTakedownsInAllLanesEarlyJungleAsLaner",
    "goldPerMinute",
    "hadAfkTeammate",
    "hadOpenNexus",
    "immobilizeAndKillWithAlly",
    "initialBuffCount",
    "initialCrabCount",
    "jungleCsBefore10Minutes",
    "junglerTakedownsNearDamagedEpicMonster",
    "kTurretsDestroyedBeforePlatesFall",
    "kda",
    "killAfterHiddenWithAlly",
    "killParticipation",
    "killedChampTookFullTeamDamageSurvived",
    "killingSprees",
    "killsNearEnemyTurret",
    "killsOnOtherLanesEarlyJungleAsLaner",
    "killsOnRecentlyHealedByAramPack",
    "killsUnderOwnTurret",
    "killsWithHelpFromEpicMonster",
    "knockEnemyIntoTeamAndKill",
    "landSkillShotsEarlyGame",
    "laneMinionsFirst10Minutes",
    "laningPhaseGoldExpAdvantage",
    "legendaryCount",
    "lostAnInhibitor",
    "maxCsAdvantageOnLaneOpponent",
    "maxKillDeficit",
    "maxLevelLeadLaneOpponent",
    "moreEnemyJungleThanOpponent",
    "multiKillOneSpell",
    "multiTurretRiftHeraldCount",
    "multikills",
    "multikillsAfterAggressiveFlash",
    "mythicItemUsed",
    "outerTurretExecutesBefore10Minutes",
    "outnumberedKills",
    "outnumberedNexusKill",
    "perfectDragonSoulsTaken",
    "perfectGame",
    "pickKillWithAlly",
    "poroExplosions",
    "quickCleanse",
    "quickFirstTurret",
    "quickSoloKills",
    "riftHeraldTakedowns",
    "saveAllyFromDeath",
    "scuttleCrabKills",
    "shortestTimeToAceFromFirstTakedown",
    "skillshotsDodged",
    "skillshotsHit",
    "snowballsHit",
    "soloBaronKills",
    "soloKills",
    "soloTurretsLategame",
    "stealthWardsPlaced",
    "survivedSingleDigitHpCount",
    "survivedThreeImmobilizesInFight",
    "takedownOnFirstTurret",
    "takedowns",
    "takedownsAfterGainingLevelAdvantage",
    "takedownsBeforeJungleMinionSpawn",
    "takedownsFirstXMinutes",
    "takedownsInAlcove",
    "takedownsInEnemyFountain",
    "teamBaronKills",
    "teamDamagePercentage",
    "teamElderDragonKills",
    "teamRiftHeraldKills",
    "thirdInhibitorDestroyedTime",
    "threeWardsOneSweeperCount",
    "tookLargeDamageSurvived",
    "turretPlatesTaken",
    "turretTakedowns",
    "turretsTakenWithRiftHerald",
    "twentyMinionsIn3SecondsCount",
    "unseenRecalls",
    "visionScoreAdvantageLaneOpponent",
    "visionScorePerMinute",
    "wardTakedowns",
    "wardTakedownsBefore20M",
    "wardsGuarded",
]

central_elements = [
    "gameId",
    "platformId",
    "gameDuration",
    "gameStartTimestamp",
    "gameCreation",
    "mapId",
    "queueId",
]
player_elements = [
    # Central but not in central data
    "win",
    "gameEndedInEarlySurrender",
    "gameEndedInSurrender",
    #  Player
    "pickTurn",
    "ban",
    "assists",
    "puuid",
    "individualPosition",
    "teamPosition",
    # Basics,
    # # "championId",
    "experience",
    "bountyLevel",
    "baronKills",
    "kills",
    "deaths",
    # Misc,
    "longestTimeSpentLiving",
    "teamEarlySurrendered",
    "totalTimeSpentDead",
    "largestCriticalStrike",
    "timeCCingOthers",
    "totalTimeCCDealt",
    # -kills/-assists/-takedowns,
    "firstBloodAssist",
    "firstBloodKill",
    "doubleKills",
    "tripleKills",
    "quadraKills",
    "pentaKills",
    "unrealKills",
    "killingSprees",
    "largestMultiKill",
    "largestKillingSpree",
    # Farming,
    "goldEarned",
    "goldSpent",
    "neutralMinionsKilled",
    "totalMinionsKilled",
    # Objectives,
    "dragonKills",
    "firstTowerAssist",
    "firstTowerKill",
    "inhibitorKills",
    "inhibitorTakedowns",
    "inhibitorsLost",
    "objectivesStolen",
    "objectivesStolenAssists",
    "turretKills",
    "turretTakedowns",
    "turretsLost",
    # Items,
    "item0",
    "item1",
    "item2",
    "item3",
    "item4",
    "item5",
    "item6",
    "consumablesPurchased",
    "itemsPurchased",
    # Perks,
    # # "statPerks",
    # # "primaryStyle",
    # # "subStyle",
    # Wards,
    "visionScore",
    "visionWardsBoughtInGame",
    "wardsKilled",
    "wardsPlaced",
    "detectorWardsPlaced",
    # Spells,
    "spell1Casts",
    "spell2Casts",
    "spell3Casts",
    "spell4Casts",
    "summoner1Casts",
    "summoner1Id",
    "summoner2Casts",
    "summoner2Id",
    "damageDealtToBuildings",
    "damageDealtToObjectives",
    "damageDealtToTurrets",
    "damageSelfMitigated",
    "magicDamageDealt",
    "magicDamageDealtToChampions",
    "magicDamageTaken",
    "physicalDamageDealt",
    "physicalDamageDealtToChampions",
    "physicalDamageTaken",
    "totalDamageDealt",
    "totalDamageDealtToChampions",
    "totalDamageShieldedOnTeammates",
    "totalDamageTaken",
    "totalHeal",
    "totalHealsOnTeammates",
    "trueDamageDealt",
    "trueDamageDealtToChampions",
    "trueDamageTaken",
]


async def generate_central(info):
    central = {}
    for entry in central_elements:
        central[entry] = info.get(entry, None)
    central["patch"] = float(".".join(info["gameVersion"].split(".")[:2]))
    central["platformId"] = central["platformId"].upper()
    return central


async def generate_player(player, bans, central):
    """Generate a dataset for a specific player to be inserted in the player table."""
    player_data = {}
    for key in player_elements:
        player_data[key] = player.get(key, None)
    challenges = player.get("challenges", {})
    for key in challenges_420:
        if key not in player_data:
            val = challenges.get(key, None)
            # if type(val) == float:
            #    val = round(val, 5)
            if val == 0:
                val = None
            player_data[key] = val
    player_data["individualPosition"] = player.get("individualPosition", "")[:1]
    player_data["teamPosition"] = player.get("teamPosition", "")[:1]
    player_data["championId"] = (
        player.get("championId") + player.get("championTransform", 0) / 100.0
    )
    perks = player.get("perks", {})
    stat_perks = perks.get("statPerks")
    stat_selection = [str(stat_perks[key]) for key in sorted(list(stat_perks.keys()))]

    player_data["statPerks"] = (
        int(
            blake2b("-".join(stat_selection).encode(), digest_size=2).hexdigest(),
            base=16,
        )
        - 32768
    )
    for style in perks.get("styles"):
        tree_selection = sorted(
            [str(entry["perk"]) for entry in style.get("selections")]
        )
        player_data[style["description"]] = (
            int(
                blake2b("-".join(tree_selection).encode(), digest_size=2).hexdigest(),
                base=16,
            )
            - 32768
        )
    player_data["ban"] = bans.get(player["participantId"], None)
    player_data = {**player_data, **central}
    for dt_var in [
        "gameStartTimestamp",
        "gameCreation",
    ]:
        player_data[dt_var] = datetime.fromtimestamp(player_data[dt_var] / 1000)
    return player_data
