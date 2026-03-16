from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .config import AppConfig, load_mal_secrets
from .db import list_series_mappings, replace_mal_anime_relations, upsert_mal_anime_metadata
from .mal_client import MalClient

DETAIL_FIELDS = (
    "id,title,alternative_titles,media_type,status,num_episodes,mean,popularity,start_season,related_anime"
)


@dataclass(slots=True)
class MetadataRefreshSummary:
    considered: int
    refreshed: int

    def as_dict(self) -> dict[str, Any]:
        return {"considered": self.considered, "refreshed": self.refreshed}


def refresh_recommendation_metadata(config: AppConfig, *, limit: int | None = None) -> MetadataRefreshSummary:
    mappings = list_series_mappings(config.db_path, provider="crunchyroll", approved_only=False)
    anime_ids = sorted({int(mapping.mal_anime_id) for mapping in mappings})
    if limit is not None and limit > 0:
        anime_ids = anime_ids[:limit]

    client = MalClient(config, load_mal_secrets(config))
    refreshed = 0
    for anime_id in anime_ids:
        details = client.get_anime_details(anime_id, fields=DETAIL_FIELDS)
        alternative_titles = details.get("alternative_titles") or {}
        aliases: list[str] = []
        if isinstance(alternative_titles, dict):
            for key in ("en", "ja"):
                value = alternative_titles.get(key)
                if isinstance(value, str) and value.strip():
                    aliases.append(value.strip())
            synonyms = alternative_titles.get("synonyms")
            if isinstance(synonyms, list):
                for value in synonyms:
                    if isinstance(value, str) and value.strip():
                        aliases.append(value.strip())

        upsert_mal_anime_metadata(
            config.db_path,
            mal_anime_id=anime_id,
            title=str(details.get("title") or anime_id),
            title_english=alternative_titles.get("en") if isinstance(alternative_titles, dict) else None,
            title_japanese=alternative_titles.get("ja") if isinstance(alternative_titles, dict) else None,
            alternative_titles=aliases,
            media_type=str(details.get("media_type")) if details.get("media_type") else None,
            status=str(details.get("status")) if details.get("status") else None,
            num_episodes=int(details["num_episodes"]) if isinstance(details.get("num_episodes"), int) else None,
            mean=float(details["mean"]) if isinstance(details.get("mean"), (float, int)) else None,
            popularity=int(details["popularity"]) if isinstance(details.get("popularity"), int) else None,
            start_season=details.get("start_season") if isinstance(details.get("start_season"), dict) else None,
            raw=details,
        )
        relations_payload: list[dict[str, Any]] = []
        for relation in details.get("related_anime") or []:
            if not isinstance(relation, dict):
                continue
            node = relation.get("node") or {}
            if not isinstance(node, dict) or not isinstance(node.get("id"), int):
                continue
            relation_type = relation.get("relation_type")
            if not isinstance(relation_type, str) or not relation_type:
                continue
            relations_payload.append(
                {
                    "related_mal_anime_id": int(node["id"]),
                    "relation_type": relation_type,
                    "relation_type_formatted": relation.get("relation_type_formatted"),
                    "related_title": node.get("title") if isinstance(node.get("title"), str) else None,
                    "raw": relation,
                }
            )
        replace_mal_anime_relations(config.db_path, mal_anime_id=anime_id, relations=relations_payload)
        refreshed += 1
    return MetadataRefreshSummary(considered=len(anime_ids), refreshed=refreshed)
