#!/usr/bin/env bash

Platforms="EUW1 EUN1 TR1 RU NA1 BR1 LA1 LA2 OC1 KR JP1"
# Files to set up for per server
for val in $Platforms; do

  # Rankings
  new_name="01_ranking_$val.sql"
  cp  startup_script_templates/01_ranking.sql "./$new_name"
  sed -i "s/PLATFORM/$val/" $new_name

  # Schemas
  new_name="00_schemas_$val.sql"
  cp  startup_script_templates/00_schemas.sql "./$new_name"
  sed -i "s/PLATFORM/$val/" $new_name

  # Participant
  new_name="04_participant_$val.sql"
  cp  startup_script_templates/04_participant.sql "./$new_name"
  sed -i "s/PLATFORM/$val/" $new_name

done

Regions="europe americas asia"
# Files to set up for per region
for val in $Regions; do

  # Schemas
  new_name="02_schemas_regions_$val.sql"
  cp  startup_script_templates/02_schemas_regions.sql "./$new_name"
  sed -i "s/REGION/$val/" $new_name

  # Matches
  new_name="03_match_$val.sql"
  cp  startup_script_templates/03_match.sql "./$new_name"
  sed -i "s/REGION/$val/" $new_name


done
