!connect jdbc:polyphenydbembedded:model=target/test-classes/wiki.json admin admin

values 'What are the largest cities in California?';
select c."Rank", c."City", c."State", c."Population" "City Population", s."Population" "State Population", (100 * c."Population" / s."Population") "Pct State Population" from "Cities" c, "States" s where c."State" = s."State" and s."State" = 'California';

values 'What percentage of California residents live in big cities?';
select count(*) "City Count", sum(100 * c."Population" / s."Population") "Pct State Population" from "Cities" c, "States" s where c."State" = s."State" and s."State" = 'California';

values 'What cities comprise the largest percentage of state population?';
select c."Rank", c."City", c."State", c."Population" "City Population", s."Population" "State Population", (100 * c."Population" / s."Population") "Pct State Population" from "Cities" c, "States" s where c."State" = s."State" order by "Pct State Population" desc limit 10;
