from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import MalSecrets, load_config
from mal_updater.db import (
    bootstrap_database,
    connect,
    get_series_mapping,
    list_review_queue_entries,
    list_series_mappings,
    upsert_mal_anime_detail_cache,
    upsert_mal_anime_metadata,
    upsert_recommendation_provider_eligibility_evidence,
    upsert_series_mapping,
)
from mal_updater.ingestion import ingest_snapshot_payload
from mal_updater.mal_client import MAL_DETAIL_CACHE_LOGIC_VERSION, MalApiError, MalClient
from mal_updater.mapping import (
    SeriesMappingInput,
    build_search_queries,
    extract_provider_mapping_evidence,
    map_series,
    normalize_title,
    should_auto_approve_mapping,
    _extract_title_hints,
)
from mal_updater.sync_planner import (
    MAPPING_REVIEW_HEURISTICS_REVISION,
    MappingReviewItem,
    SyncProposal,
    build_dry_run_sync_plan,
    build_mapping_review,
    execute_approved_sync,
    persist_mapping_review_queue,
    persist_sync_review_queue,
)
from tests.test_validation_ingestion import sample_snapshot


def _write_test_mal_secret_files(root: Path) -> None:
    (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
    (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
    (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")


def _replace_progress_with_completed_episodes(payload: dict, provider_series_id: str, episode_count: int) -> None:
    progress_template = dict(payload["progress"][0])
    payload["progress"] = []
    for episode_number in range(1, episode_count + 1):
        item = dict(progress_template)
        item["provider_series_id"] = provider_series_id
        item["provider_episode_id"] = f"ep-{episode_number}"
        item["episode_number"] = episode_number
        item["completion_ratio"] = 1.0
        payload["progress"].append(item)


def _misfit_split_part_search_response() -> dict:
    return {
        "data": [
            {
                "node": {
                    "id": 48417,
                    "title": "Maou Gakuin no Futekigousha II: Shijou Saikyou no Maou no Shiso, Tensei shite Shison-tachi no Gakkou e Kayou",
                    "alternative_titles": {"en": "The Misfit of Demon King Academy II"},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 12,
                }
            },
            {
                "node": {
                    "id": 48418,
                    "title": "Maou Gakuin no Futekigousha II: Shijou Saikyou no Maou no Shiso, Tensei shite Shison-tachi no Gakkou e Kayou Part 2",
                    "alternative_titles": {"en": "The Misfit of Demon King Academy II Part 2"},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 12,
                }
            },
            {
                "node": {
                    "id": 40496,
                    "title": "Maou Gakuin no Futekigousha: Shijou Saikyou no Maou no Shiso, Tensei shite Shison-tachi no Gakkou e Kayou",
                    "alternative_titles": {"en": "The Misfit of Demon King Academy"},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 13,
                }
            },
        ]
    }


class MappingTests(unittest.TestCase):
    def _map_with_search_results(self, series: SeriesMappingInput, nodes: list[dict]):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )
            with patch.object(MalClient, "search_anime", return_value={"data": [{"node": node} for node in nodes]}):
                return map_series(client, series)

    def _dororo_search_nodes(self) -> list[dict]:
        return [
            {
                "id": 5760,
                "title": "Dororo to Hyakkimaru",
                "alternative_titles": {"en": "Dororo", "synonyms": ["Dororo"]},
                "media_type": "tv",
                "status": "finished_airing",
                "num_episodes": 26,
                "start_season": {"season": "spring", "year": 1969},
            },
            {
                "id": 37520,
                "title": "Dororo",
                "alternative_titles": {"en": "Dororo", "synonyms": ["Dororo to Hyakkimaru"]},
                "media_type": "tv",
                "status": "finished_airing",
                "num_episodes": 24,
                "start_season": {"season": "winter", "year": 2019},
            },
        ]

    def test_normalize_title_strips_dub_and_season_noise(self) -> None:
        self.assertEqual(
            normalize_title("BOFURI: I Don’t Want to Get Hurt, so I’ll Max Out My Defense. Season 2 (English Dub)"),
            "bofuri i don t want to get hurt so i ll max out my defense",
        )

    def test_normalize_title_splits_letter_digit_boundaries(self) -> None:
        self.assertEqual(normalize_title("PERSONA5 the Animation"), "persona 5 the animation")
        self.assertEqual(normalize_title("Ver1.1a"), "ver 1 1 a")

    def test_auxiliary_provider_movie_season_stays_review_gated_against_tv_series(self) -> None:
        result = self._map_with_search_results(
            SeriesMappingInput(
                provider="crunchyroll",
                provider_series_id="konosuba-movie-season",
                title="KONOSUBA -God's blessing on this wonderful world!",
                season_title="Movie Season 1",
                season_number=1,
                max_episode_number=11,
                completed_episode_count=11,
            ),
            [
                {
                    "id": 38040,
                    "title": "Kono Subarashii Sekai ni Shukufuku wo! Movie: Kurenai Densetsu",
                    "alternative_titles": {"en": "KONOSUBA -God's blessing on this wonderful world!- Legend of Crimson"},
                    "media_type": "movie",
                    "num_episodes": 1,
                },
                {
                    "id": 30831,
                    "title": "Kono Subarashii Sekai ni Shukufuku wo!",
                    "alternative_titles": {"en": "KONOSUBA -God's blessing on this wonderful world!"},
                    "media_type": "tv",
                    "num_episodes": 10,
                },
            ],
        )
        self.assertFalse(should_auto_approve_mapping(result))
        self.assertEqual(result.chosen_candidate.mal_anime_id, 30831)

    def test_crunchyroll_season_metadata_title_conflict_blocks_auto_approval(self) -> None:
        result = self._map_with_search_results(
            SeriesMappingInput(
                provider="crunchyroll",
                provider_series_id="metadata-title-conflict",
                title="Example Show",
                season_title="Example Show Season 2",
                season_number=1,
                max_episode_number=12,
            ),
            [
                {
                    "id": 202,
                    "title": "Example Show Season 2",
                    "alternative_titles": {"en": "Example Show Season 2"},
                    "media_type": "tv",
                    "num_episodes": 12,
                },
                {
                    "id": 101,
                    "title": "Example Show",
                    "alternative_titles": {"en": "Example Show"},
                    "media_type": "tv",
                    "num_episodes": 12,
                },
            ],
        )

        self.assertFalse(should_auto_approve_mapping(result))
        self.assertTrue(
            any(reason.startswith("provider_season_metadata_conflict=metadata:1;title:2") for reason in result.chosen_candidate.match_reasons)
        )

    def test_split_cour_bundle_evidence_remains_review_when_entries_are_separate_mal_nodes(self) -> None:
        result = self._map_with_search_results(
            SeriesMappingInput(
                provider="crunchyroll",
                provider_series_id="split-cour-bundle",
                title="Example Saga",
                season_title="Example Saga Season 2",
                season_number=2,
                max_episode_number=24,
                completed_episode_count=24,
            ),
            [
                {
                    "id": 2001,
                    "title": "Example Saga 2nd Season Part 1",
                    "alternative_titles": {"en": "Example Saga Season 2"},
                    "media_type": "tv",
                    "num_episodes": 12,
                },
                {
                    "id": 2002,
                    "title": "Example Saga 2nd Season Part 2",
                    "alternative_titles": {"en": "Example Saga Season 2 Part 2"},
                    "media_type": "tv",
                    "num_episodes": 12,
                },
            ],
        )

        self.assertFalse(should_auto_approve_mapping(result))
        self.assertTrue(
            any(reason.startswith("aggregated_episode_numbering_suspected=24>12") for reason in result.chosen_candidate.match_reasons)
        )

    def test_exact_tv_candidate_wins_over_unknown_duplicate_title(self) -> None:
        result = self._map_with_search_results(
            SeriesMappingInput(
                provider="crunchyroll",
                provider_series_id="world-trigger",
                title="World Trigger",
                season_title="World Trigger (English Dub)",
                season_number=1,
            ),
            [
                {"id": 63048, "title": "World Trigger (Unknown)", "media_type": "unknown", "num_episodes": None},
                {"id": 24405, "title": "World Trigger", "media_type": "tv", "num_episodes": 73},
            ],
        )

        self.assertEqual(result.status, "exact")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 24405)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_exact_tv_candidate_wins_over_movie_duplicate_title(self) -> None:
        result = self._map_with_search_results(
            SeriesMappingInput(
                provider="crunchyroll",
                provider_series_id="afro-samurai",
                title="Afro Samurai",
                season_title="Afro Samurai",
                season_number=1,
                max_episode_number=5,
            ),
            [
                {"id": 13709, "title": "Afro Samurai Movie", "media_type": "movie", "num_episodes": 1},
                {"id": 1292, "title": "Afro Samurai", "media_type": "tv", "num_episodes": 5},
            ],
        )

        self.assertEqual(result.status, "exact")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 1292)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_exact_provider_season_subtitle_beats_base_title_trim(self) -> None:
        result = self._map_with_search_results(
            SeriesMappingInput(
                provider="crunchyroll",
                provider_series_id="blue-exorcist-kyoto",
                title="Blue Exorcist",
                season_title="Blue Exorcist: Kyoto Saga",
                season_number=1,
                max_episode_number=25,
            ),
            [
                {
                    "id": 9919,
                    "title": "Ao no Exorcist",
                    "alternative_titles": {"en": "Blue Exorcist", "synonyms": []},
                    "media_type": "tv",
                    "num_episodes": 25,
                },
                {
                    "id": 33506,
                    "title": "Ao no Exorcist: Kyoto Fujouou-hen",
                    "alternative_titles": {"en": "Blue Exorcist: Kyoto Saga", "synonyms": []},
                    "media_type": "tv",
                    "num_episodes": 12,
                },
            ],
        )

        self.assertEqual(result.status, "ambiguous")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 33506)
        self.assertIn("exact_provider_season_title", result.chosen_candidate.match_reasons)
        self.assertFalse(should_auto_approve_mapping(result))

    def test_exact_provider_season_title_beats_generic_primary_title_collision_for_flcl(self) -> None:
        with patch.object(MalClient, "get_anime_details", side_effect=MalApiError("offline")):
            result = self._map_with_search_results(
                SeriesMappingInput(
                    provider="crunchyroll",
                    provider_series_id="flcl-alternative",
                    title="FLCL",
                    season_title="FLCL Alternative",
                    season_number=1,
                ),
                [
                    {
                        "id": 227,
                        "title": "FLCL",
                        "alternative_titles": {"en": "FLCL", "synonyms": ["Fooly Cooly", "Furi Kuri"]},
                        "media_type": "ova",
                        "num_episodes": 6,
                    },
                    {
                        "id": 35842,
                        "title": "FLCL Alternative",
                        "alternative_titles": {"en": "FLCL Alternative", "synonyms": ["FLCL 3", "Fooly Cooly Alternative"]},
                        "media_type": "movie",
                        "num_episodes": 1,
                    },
                ],
            )

        self.assertEqual("exact", result.status)
        self.assertEqual(35842, result.chosen_candidate.mal_anime_id)
        self.assertIn("exact_provider_season_title", result.chosen_candidate.match_reasons)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_multi_episode_ona_exact_english_title_can_auto_approve_when_episode_evidence_fits(self) -> None:
        result = self._map_with_search_results(
            SeriesMappingInput(
                provider="crunchyroll",
                provider_series_id="last-summoner",
                title="The Last Summoner",
                season_title="The Last Summoner",
                season_number=1,
                max_episode_number=10,
                completed_episode_count=8,
            ),
            [
                {
                    "id": 41915,
                    "title": "Zuihou de Zhaohuan Shi",
                    "alternative_titles": {"en": "The Last Summoner", "synonyms": ["Zui Hou De Zhao Huan Shi"]},
                    "media_type": "ona",
                    "num_episodes": 12,
                },
                {"id": 90001, "title": "The Last Spell", "alternative_titles": {}, "media_type": "tv", "num_episodes": 12},
            ],
        )

        self.assertEqual("exact", result.status)
        self.assertEqual(41915, result.chosen_candidate.mal_anime_id)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_exact_title_tie_without_provider_disambiguation_is_terminal_non_actionable(self) -> None:
        result = self._map_with_search_results(
            SeriesMappingInput(
                provider="hidive",
                provider_series_id="1181",
                title="Dororo",
                season_title="Dororo",
            ),
            self._dororo_search_nodes(),
        )

        self.assertEqual("ambiguous", result.status)
        self.assertTrue(result.has_deterministic_ambiguous_exact_title_classification())
        self.assertFalse(should_auto_approve_mapping(result))

    def test_aggregate_progress_overflow_is_terminal_non_actionable_without_bundle_companions(self) -> None:
        result = self._map_with_search_results(
            SeriesMappingInput(
                provider="crunchyroll",
                provider_series_id="blue-night-saga",
                title="Blue Exorcist",
                season_title="Blue Exorcist -The Blue Night Saga-",
                season_number=5,
                max_episode_number=25,
                completed_episode_count=70,
            ),
            [
                {
                    "id": 59226,
                    "title": "Ao no Exorcist: Yosuga-hen",
                    "alternative_titles": {"en": "Blue Exorcist: The Blue Night Saga", "synonyms": ["Blue Exorcist Season 5"]},
                    "media_type": "tv",
                    "num_episodes": 12,
                }
            ],
        )

        self.assertEqual("exact", result.status)
        self.assertTrue(result.has_deterministic_aggregate_progress_classification())
        self.assertFalse(should_auto_approve_mapping(result))

    def test_map_series_does_not_treat_large_terminal_unit_number_as_installment(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 56009,
                                "title": "Yuusha-kei ni Shosu: Choubatsu Yuusha 9004-tai Keimu Kiroku",
                                "alternative_titles": {
                                    "en": "Sentenced to Be a Hero",
                                    "synonyms": ["Sentenced to Be a Hero: The Prison Records of Penal Hero Unit 9004"],
                                },
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 63820,
                                "title": "Yuusha-kei ni Shosu: Choubatsu Yuusha 9004-tai Keimu Kiroku 2nd Season",
                                "alternative_titles": {"en": "Sentenced to Be a Hero Season 2", "synonyms": []},
                                "media_type": "tv",
                                "status": "not_yet_aired",
                                "num_episodes": 0,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Sentenced to Be a Hero",
                        season_title="Season 1",
                        season_number=1,
                        max_episode_number=10,
                        completed_episode_count=9,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertTrue(should_auto_approve_mapping(result))
        self.assertEqual(result.chosen_candidate.mal_anime_id, 56009)
        self.assertNotIn("season_number_mismatch=provider:1;candidate:9004", result.rationale)

    def test_map_series_does_not_treat_level_or_lv_title_numeral_as_installment(self) -> None:
        result = self._map_with_search_results(
            SeriesMappingInput(
                provider="crunchyroll",
                provider_series_id="GEXH3W2PK",
                title="Chillin’ in Another World with Level 2 Super Cheat Powers",
                season_title="Chillin’ in Another World with Level 2 Super Cheat Powers",
                season_number=1,
                max_episode_number=12,
                completed_episode_count=12,
            ),
            [
                {
                    "id": 56923,
                    "title": "Lv2 kara Cheat datta Motoyuusha Kouho no Mattari Isekai Life",
                    "alternative_titles": {"en": "Chillin’ in Another World with Level 2 Super Cheat Powers", "synonyms": []},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 12,
                },
                {
                    "id": 64553,
                    "title": "Lv2 kara Cheat datta Motoyuusha Kouho no Mattari Isekai Life 2nd Season",
                    "alternative_titles": {"en": "Chillin’ in Another World with Level 2 Super Cheat Powers Season 2", "synonyms": []},
                    "media_type": "tv",
                    "status": "not_yet_aired",
                    "num_episodes": 0,
                },
                {
                    "id": 40960,
                    "title": "Cheat Kusushi no Slow Life: Isekai ni Tsukurou Drugstore",
                    "alternative_titles": {"en": "Drug Store in Another World - The Slow Life of a Cheat Pharmacist", "synonyms": []},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 12,
                },
                {
                    "id": 35203,
                    "title": "Isekai wa Smartphone to Tomo ni.",
                    "alternative_titles": {"en": "In Another World With My Smartphone", "synonyms": []},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 12,
                },
                {
                    "id": 15315,
                    "title": "Mondaiji-tachi ga Isekai kara Kuru Sou desu yo?",
                    "alternative_titles": {"en": "Problem Children Are Coming from Another World, Aren't They?", "synonyms": []},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 10,
                },
            ],
        )

        self.assertEqual(result.status, "exact")
        self.assertTrue(should_auto_approve_mapping(result))
        self.assertEqual(result.chosen_candidate.mal_anime_id, 56923)
        self.assertIn("exact_normalized_title", result.rationale)
        self.assertFalse(any(reason.startswith("season_number_mismatch=provider:1;candidate:2") for reason in result.rationale))
        self.assertNotIn("candidate_extra_installment_hint", result.rationale)
        self.assertFalse(any(reason.startswith("season_number_mismatch=provider:1;candidate:2") for reason in result.chosen_candidate.match_reasons))
        self.assertNotIn("candidate_extra_installment_hint", result.chosen_candidate.match_reasons)

    def test_title_domain_level_lv_rank_class_grade_numerals_are_not_installment_hints(self) -> None:
        for title in (
            "Chillin’ in Another World with Level 2 Super Cheat Powers",
            "Lv2 kara Cheat datta Motoyuusha Kouho no Mattari Isekai Life",
            "Example Level 2",
            "Example Lv2",
            "Example Rank 2",
            "Example Class 2",
            "Example Grade 2",
        ):
            with self.subTest(title=title):
                self.assertFalse(_extract_title_hints(title) & {"season:2", "roman:2"})


    def test_title_domain_sentence_terminal_ii_is_not_installment_hint(self) -> None:
        self.assertFalse(
            _extract_title_hints("Maou no Ore ga Dorei Elf wo Yome ni Shitanda ga, Dou Medereba Ii?")
            & {"season:2", "roman:2"}
        )
        self.assertFalse(
            _extract_title_hints("An Archdemon's Dilemma: How to Love Your Elf Bride")
            & {"season:2", "roman:2"}
        )

    def test_true_installment_hints_remain_recognized(self) -> None:
        self.assertIn("season:2", _extract_title_hints("Example Show Season 2"))
        self.assertIn("season:2", _extract_title_hints("Example Show 2nd Season"))
        self.assertIn("part:2", _extract_title_hints("Example Show Part 2"))
        self.assertIn("split:2", _extract_title_hints("Example Show Part 2"))
        self.assertIn("roman:2", _extract_title_hints("Example Show II"))
        self.assertIn("season:2", _extract_title_hints("Example Show Second Stage"))
        self.assertIn("season:2", _extract_title_hints("Example Show Second Beat"))

    def test_high_confidence_english_alias_match_can_auto_approve_when_episode_evidence_fits(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 39523,
                                "title": "Choujin Koukousei-tachi wa Isekai demo Yoyuu de Ikinuku you desu!",
                                "alternative_titles": {
                                    "en": "CHOYOYU!: High School Prodigies Have It Easy Even in Another World!",
                                    "synonyms": ["Super Human High Schoolers Are in Another World, But Seem to be Living in Comfort!"],
                                },
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 34012,
                                "title": "Isekai Shokudou",
                                "alternative_titles": {"en": "Restaurant to Another World", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="High School Prodigies Have It Easy Even In Another World",
                        season_title="High School Prodigies Have It Easy Even In Another World (English Dub)",
                        season_number=1,
                        max_episode_number=12,
                        completed_episode_count=11,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertTrue(should_auto_approve_mapping(result))
        self.assertEqual(result.chosen_candidate.mal_anime_id, 39523)

    def test_high_confidence_english_alias_match_stays_unapproved_when_episode_evidence_overflows(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 39523,
                                "title": "Choujin Koukousei-tachi wa Isekai demo Yoyuu de Ikinuku you desu!",
                                "alternative_titles": {"en": "CHOYOYU!: High School Prodigies Have It Easy Even in Another World!", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="High School Prodigies Have It Easy Even In Another World",
                        season_title="High School Prodigies Have It Easy Even In Another World (English Dub)",
                        season_number=1,
                        max_episode_number=24,
                        completed_episode_count=24,
                    ),
                )

        self.assertFalse(should_auto_approve_mapping(result))
        self.assertIn("episode_evidence_exceeds_candidate_count=24>12", result.rationale)

    def test_supplemental_title_candidate_recovers_mal_search_alias_gap(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 1251,
                                "title": "Fushigi no Umi no Nadia",
                                "alternative_titles": {"en": "Nadia: The Secret of Blue Water", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 39,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 58473,
                    "title": "S-Rank Monster no \"Behemoth\" dakedo, Neko to Machigawarete Elf Musume no Pet toshite Kurashitemasu",
                    "alternative_titles": {"en": "Beheneko: The Elf-Girl's Cat is Secretly an S-Ranked Monster!", "synonyms": []},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 12,
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="hidive",
                        provider_series_id="series-123",
                        title="Beheneko: The Elf-Girl's Cat is Secretly an S-Ranked Monster!",
                        season_title="Season 1",
                        season_number=1,
                        max_episode_number=8,
                        completed_episode_count=0,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertTrue(should_auto_approve_mapping(result))
        self.assertEqual(result.chosen_candidate.mal_anime_id, 58473)
        self.assertIn("supplemental_title_candidate", result.rationale)

    def test_map_series_does_not_treat_stylized_single_x_as_roman_installment_hint(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 30911,
                                "title": "Tales of Zestiria the Cross",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 34086,
                                "title": "Tales of Zestiria the Cross 2nd Season",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 13,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Tales of Zestiria the X",
                        season_title="Tales of Zestiria the X (English Dub)",
                        season_number=1,
                        max_episode_number=16,
                        completed_episode_count=16,
                    ),
                )

        self.assertEqual(result.chosen_candidate.mal_anime_id, 30911)
        self.assertFalse(any(reason.startswith("roman_installment_match=") for reason in result.rationale))
        self.assertFalse(any(reason.startswith("installment_hint_match=roman:") for reason in result.rationale))
        self.assertFalse(any(reason.startswith("roman_installment_match=") for reason in result.candidates[0].match_reasons))

    def test_map_series_flags_exact_title_overflow_as_possible_multi_entry_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 849,
                                "title": "Suzumiya Haruhi no Yuuutsu",
                                "alternative_titles": {"en": "The Melancholy of Haruhi Suzumiya", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 14,
                            }
                        },
                        {
                            "node": {
                                "id": 4382,
                                "title": "Suzumiya Haruhi no Yuuutsu (2009)",
                                "alternative_titles": {"en": "The Melancholy of Haruhi Suzumiya", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 14,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="The Melancholy of Haruhi Suzumiya",
                        season_title="The Melancholy of Haruhi Suzumiya (English Dub)",
                        season_number=1,
                        max_episode_number=28,
                        completed_episode_count=28,
                    ),
                )

        self.assertEqual(result.status, "ambiguous")
        self.assertTrue(result.has_deterministic_ambiguous_exact_title_classification())
        self.assertFalse(should_auto_approve_mapping(result))
        self.assertEqual(849, result.chosen_candidate.mal_anime_id)
        self.assertIn("exact_normalized_title", result.rationale)
        self.assertIn("episode_evidence_exceeds_candidate_count=28>14", result.rationale)
        self.assertIn("multi_entry_bundle_suspected=28<=14+14", result.rationale)
        self.assertIsNotNone(result.bundle_companion_candidate)
        self.assertEqual(4382, result.bundle_companion_candidate.mal_anime_id)
        self.assertEqual(1, len(result.bundle_companion_candidates or []))
        self.assertEqual({4382}, {candidate.mal_anime_id for candidate in (result.bundle_companion_candidates or [])})

    def test_map_series_flags_exact_title_overflow_as_bundle_even_when_later_season_companion_scores_low(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 20057,
                                "title": "Space☆Dandy",
                                "alternative_titles": {"en": "Space Dandy", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 13,
                            }
                        },
                        {
                            "node": {
                                "id": 2451,
                                "title": "Space Cobra",
                                "alternative_titles": {"en": "Space Cobra", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 31,
                            }
                        },
                        {
                            "node": {
                                "id": 12431,
                                "title": "Uchuu Kyoudai",
                                "alternative_titles": {"en": "Space Brothers", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 99,
                            }
                        },
                        {
                            "node": {
                                "id": 2452,
                                "title": "Space Adventure Cobra",
                                "alternative_titles": {"en": "Space Adventure Cobra", "synonyms": []},
                                "media_type": "movie",
                                "status": "finished_airing",
                                "num_episodes": 1,
                            }
                        },
                        {
                            "node": {
                                "id": 23327,
                                "title": "Space☆Dandy 2nd Season",
                                "alternative_titles": {"en": "Space Dandy 2nd Season", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 13,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-space-dandy",
                        title="Space Dandy",
                        season_title="Space Dandy (English Dub)",
                        season_number=1,
                        max_episode_number=21,
                        completed_episode_count=21,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 20057)
        self.assertIn("episode_evidence_exceeds_candidate_count=21>13", result.rationale)
        self.assertIn("multi_entry_bundle_suspected=21<=13+13", result.rationale)
        self.assertIsNotNone(result.bundle_companion_candidate)
        self.assertEqual(23327, result.bundle_companion_candidate.mal_anime_id)
        self.assertEqual({23327}, {candidate.mal_anime_id for candidate in (result.bundle_companion_candidates or [])})
        self.assertTrue(result.has_deterministic_aggregate_progress_classification())
        self.assertFalse(should_auto_approve_mapping(result))

    def test_map_series_flags_alias_only_bundle_companion_for_review_without_auto_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 71001,
                                "title": "Kiseki Project",
                                "alternative_titles": {"en": "Alias Show", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 71002,
                                "title": "Shin Kiseki Project",
                                "alternative_titles": {"en": "Alias Show Season 2", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 71003,
                                "title": "Alias Show Mini Drama",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "special",
                                "status": "finished_airing",
                                "num_episodes": 1,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-alias-show",
                        title="Alias Show",
                        season_title="Alias Show (English Dub)",
                        season_number=1,
                        max_episode_number=24,
                        completed_episode_count=24,
                    ),
                )

        self.assertEqual(result.status, "ambiguous")
        self.assertEqual(71001, result.chosen_candidate.mal_anime_id)
        self.assertIn("multi_entry_bundle_suspected=24<=12+12", result.rationale)
        self.assertEqual({71002}, {candidate.mal_anime_id for candidate in (result.bundle_companion_candidates or [])})
        self.assertFalse(should_auto_approve_mapping(result))

    def test_map_series_flags_exact_title_overflow_as_possible_three_entry_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 1001,
                                "title": "Example Split Show",
                                "alternative_titles": {"en": "Example Split Show", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 1002,
                                "title": "Example Split Show Part 2",
                                "alternative_titles": {"en": "Example Split Show", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 1003,
                                "title": "Example Split Show Part 3",
                                "alternative_titles": {"en": "Example Split Show", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123b",
                        title="Example Split Show",
                        season_title="Example Split Show (English Dub)",
                        season_number=1,
                        max_episode_number=36,
                        completed_episode_count=36,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 1001)
        self.assertIn("episode_evidence_exceeds_candidate_count=36>12", result.rationale)
        self.assertIn("multi_entry_bundle_suspected=36<=12+12+12", result.rationale)
        self.assertIsNotNone(result.bundle_companion_candidate)
        self.assertIn(result.bundle_companion_candidate.mal_anime_id, {1002, 1003})
        self.assertEqual({1002, 1003}, {candidate.mal_anime_id for candidate in (result.bundle_companion_candidates or [])})
        self.assertTrue(result.has_deterministic_aggregate_progress_classification())
        self.assertFalse(should_auto_approve_mapping(result))

    def test_map_series_multi_entry_bundle_prefers_later_seasons_over_higher_scoring_sidecar_noise(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 39468,
                                "title": "Honzuki no Gekokujou: Shisho ni Naru Tame ni wa Shudan wo Erandeiraremasen",
                                "alternative_titles": {"en": "Ascendance of a Bookworm", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 14,
                            }
                        },
                        {
                            "node": {
                                "id": 40841,
                                "title": "Honzuki no Gekokujou: Shisho ni Naru Tame ni wa Shudan wo Erandeiraremasen OVA",
                                "alternative_titles": {"en": "Ascendance of a Bookworm OVA", "synonyms": []},
                                "media_type": "ova",
                                "status": "finished_airing",
                                "num_episodes": 1,
                            }
                        },
                        {
                            "node": {
                                "id": 42429,
                                "title": "Honzuki no Gekokujou: Shisho ni Naru Tame ni wa Shudan wo Erandeiraremasen 3rd Season",
                                "alternative_titles": {"en": "Ascendance of a Bookworm Season 3", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 10,
                            }
                        },
                        {
                            "node": {
                                "id": 40815,
                                "title": "Honzuki no Gekokujou: Shisho ni Naru Tame ni wa Shudan wo Erandeiraremasen 2nd Season",
                                "alternative_titles": {"en": "Ascendance of a Bookworm Season 2", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 51616,
                                "title": "Honzuki no Gekokujou: Shisho ni Naru Tame ni wa Shudan wo Erandeiraremasen Recap",
                                "alternative_titles": {"en": "Ascendance of a Bookworm Recap", "synonyms": []},
                                "media_type": "tv_special",
                                "status": "finished_airing",
                                "num_episodes": 2,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-bookworm",
                        title="Ascendance of a Bookworm",
                        season_title="Ascendance of a Bookworm (English Dub)",
                        season_number=1,
                        max_episode_number=32,
                        completed_episode_count=32,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 39468)
        self.assertIn("episode_evidence_exceeds_candidate_count=32>14", result.rationale)
        self.assertIn("multi_entry_bundle_suspected=32<=14+10+12", result.rationale)
        self.assertEqual({42429, 40815}, {candidate.mal_anime_id for candidate in (result.bundle_companion_candidates or [])})
        self.assertNotIn(40841, {candidate.mal_anime_id for candidate in (result.bundle_companion_candidates or [])})
        self.assertTrue(result.has_deterministic_aggregate_progress_classification())
        self.assertFalse(should_auto_approve_mapping(result))
        self.assertGreater(
            next(candidate.score for candidate in result.candidates if candidate.mal_anime_id == 40841),
            next(candidate.score for candidate in result.candidates if candidate.mal_anime_id == 42429),
        )

    def test_map_series_flags_explicit_later_season_overflow_as_possible_multi_entry_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 58572,
                                "title": "Shangri-La Frontier: Kusoge Hunter, Kamige ni Idoman to su 2nd Season",
                                "alternative_titles": {"en": "Shangri-La Frontier Season 2", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 25,
                            }
                        },
                        {
                            "node": {
                                "id": 61338,
                                "title": "Shangri-La Frontier: Kusoge Hunter, Kamige ni Idoman to su 3rd Season",
                                "alternative_titles": {"en": "Shangri-La Frontier Season 3", "synonyms": []},
                                "media_type": "tv",
                                "status": "not_yet_aired",
                                "num_episodes": 25,
                            }
                        },
                        {
                            "node": {
                                "id": 52347,
                                "title": "Shangri-La Frontier: Kusoge Hunter, Kamige ni Idoman to su",
                                "alternative_titles": {"en": "Shangri-La Frontier", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 25,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-shangri-bundle",
                        title="Shangri-La Frontier",
                        season_title="Shangri-La Frontier Season 2 (English Dub)",
                        season_number=2,
                        max_episode_number=49,
                        completed_episode_count=49,
                    ),
                )

        self.assertEqual(result.status, "strong")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 58572)
        self.assertIn("season_number_match=2", result.rationale)
        self.assertIn("episode_evidence_exceeds_candidate_count=49>25", result.rationale)
        self.assertIn("multi_entry_bundle_suspected=49<=25+25", result.rationale)
        self.assertEqual({61338}, {candidate.mal_anime_id for candidate in (result.bundle_companion_candidates or [])})

    def test_map_series_prefers_contiguous_followup_bundle_companion_over_skip_ahead_installment(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 58572,
                                "title": "Shangri-La Frontier: Kusoge Hunter, Kamige ni Idoman to su 2nd Season",
                                "alternative_titles": {"en": "Shangri-La Frontier Season 2", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 25,
                            }
                        },
                        {
                            "node": {
                                "id": 70003,
                                "title": "Shangri-La Frontier: Kusoge Hunter, Kamige ni Idoman to su 4th Season",
                                "alternative_titles": {"en": "Shangri-La Frontier Season 4", "synonyms": []},
                                "media_type": "tv",
                                "status": "not_yet_aired",
                                "num_episodes": 25,
                            }
                        },
                        {
                            "node": {
                                "id": 61338,
                                "title": "Shangri-La Frontier: Kusoge Hunter, Kamige ni Idoman to su 3rd Season",
                                "alternative_titles": {"en": "Shangri-La Frontier Season 3", "synonyms": []},
                                "media_type": "tv",
                                "status": "not_yet_aired",
                                "num_episodes": 25,
                            }
                        },
                        {
                            "node": {
                                "id": 52347,
                                "title": "Shangri-La Frontier: Kusoge Hunter, Kamige ni Idoman to su",
                                "alternative_titles": {"en": "Shangri-La Frontier", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 25,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-shangri-contiguous-bundle",
                        title="Shangri-La Frontier",
                        season_title="Shangri-La Frontier Season 2 (English Dub)",
                        season_number=2,
                        max_episode_number=49,
                        completed_episode_count=49,
                    ),
                )

        self.assertEqual(result.status, "strong")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 58572)
        self.assertIn("multi_entry_bundle_suspected=49<=25+25", result.rationale)
        self.assertEqual({61338}, {candidate.mal_anime_id for candidate in (result.bundle_companion_candidates or [])})

    def test_map_series_boosts_base_title_match_when_provider_title_only_adds_arc_subtitle(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            def fake_search(query: str, limit: int = 5) -> dict:
                if query == "One Piece":
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 21,
                                    "title": "One Piece",
                                    "alternative_titles": {},
                                    "media_type": "tv",
                                    "status": "currently_airing",
                                    "num_episodes": 9999,
                                }
                            },
                            {
                                "node": {
                                    "id": 22,
                                    "title": "One Room",
                                    "alternative_titles": {},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 12,
                                }
                            },
                        ]
                    }
                return {"data": []}

            with patch.object(MalClient, "search_anime", side_effect=fake_search):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-arc",
                        title="One Piece: Egghead Arc (English Dub)",
                        max_episode_number=1122,
                        completed_episode_count=1120,
                    ),
                )

        self.assertEqual(result.status, "strong")
        self.assertIsNotNone(result.chosen_candidate)
        self.assertEqual(result.chosen_candidate.mal_anime_id, 21)
        self.assertIn("exact_base_title_after_subtitle_trim", result.rationale)
        self.assertFalse(should_auto_approve_mapping(result))

    def test_map_series_does_not_trim_installment_subtitle_into_false_base_match(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 42,
                                "title": "Attack on Titan",
                                "alternative_titles": {},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 25,
                            }
                        }
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-installment",
                        title="Attack on Titan: Season 2",
                        season_number=2,
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertNotIn("exact_base_title_after_subtitle_trim", result.rationale)
        self.assertNotEqual(result.status, "exact")

    def test_map_series_falls_back_to_base_title_for_pipe_or_dash_arc_subtitles(self) -> None:
        for title in (
            "One Piece | Egghead Arc (English Dub)",
            "One Piece — Egghead Arc (English Dub)",
            "One Piece – Egghead Arc (English Dub)",
        ):
            with self.subTest(title=title):
                with tempfile.TemporaryDirectory() as td:
                    root = Path(td)
                    (root / ".MAL-Updater" / "config").mkdir(parents=True)
                    config = load_config(root)
                    client = MalClient(
                        config,
                        MalSecrets(
                            client_id="client-id",
                            client_secret=None,
                            access_token="access-token",
                            refresh_token=None,
                            client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                            client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                            access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                            refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                        ),
                    )

                    def fake_search(query: str, limit: int = 5) -> dict:
                        if query == "One Piece":
                            return {
                                "data": [
                                    {
                                        "node": {
                                            "id": 21,
                                            "title": "One Piece",
                                            "alternative_titles": {},
                                            "media_type": "tv",
                                            "status": "currently_airing",
                                            "num_episodes": 9999,
                                        }
                                    }
                                ]
                            }
                        return {"data": []}

                    with patch.object(MalClient, "search_anime", side_effect=fake_search) as search_mock:
                        result = map_series(
                            client,
                            SeriesMappingInput(
                                provider="crunchyroll",
                                provider_series_id="series-arc-delimited",
                                title=title,
                                max_episode_number=1122,
                                completed_episode_count=1120,
                            ),
                        )

                attempted_queries = [call.args[0] for call in search_mock.call_args_list]
                self.assertIn("One Piece", attempted_queries)
                self.assertEqual(result.status, "strong")
                self.assertEqual(result.chosen_candidate.mal_anime_id, 21)
                self.assertIn("exact_base_title_after_subtitle_trim", result.rationale)

    def test_map_series_penalizes_related_non_tv_sidecar_when_provider_has_explicit_season_context(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            def fake_search(query: str, limit: int = 5) -> dict:
                if query == "Million Arthur Season 2 (English Dub)":
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 38268,
                                    "title": "Hangyakusei Million Arthur 2nd Season",
                                    "alternative_titles": {},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 13,
                                }
                            }
                        ]
                    }
                if query == "Million Arthur":
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 37555,
                                    "title": "Hangyakusei Million Arthur",
                                    "alternative_titles": {},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 10,
                                }
                            }
                        ]
                    }
                return {"data": []}

            def fake_details(anime_id: int, fields: str | None = None) -> dict:
                if anime_id == 38268:
                    return {
                        "id": 38268,
                        "title": "Hangyakusei Million Arthur 2nd Season",
                        "alternative_titles": {},
                        "media_type": "tv",
                        "status": "finished_airing",
                        "num_episodes": 13,
                        "related_anime": [{"node": {"id": 30954}}],
                    }
                if anime_id == 37555:
                    return {
                        "id": 37555,
                        "title": "Hangyakusei Million Arthur",
                        "alternative_titles": {},
                        "media_type": "tv",
                        "status": "finished_airing",
                        "num_episodes": 10,
                        "related_anime": [],
                    }
                if anime_id == 30954:
                    return {
                        "id": 30954,
                        "title": "Jakusansei Million Arthur",
                        "alternative_titles": {},
                        "media_type": "ona",
                        "status": "finished_airing",
                        "num_episodes": 10,
                        "related_anime": [],
                    }
                return {"id": anime_id, "related_anime": []}

            with (
                patch.object(MalClient, "search_anime", side_effect=fake_search),
                patch.object(MalClient, "get_anime_details", side_effect=fake_details),
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-million-arthur",
                        title="Million Arthur",
                        season_title="Million Arthur Season 2 (English Dub)",
                        season_number=2,
                        completed_episode_count=13,
                        max_episode_number=13,
                    ),
                )

        self.assertIsNotNone(result.chosen_candidate)
        self.assertEqual(result.chosen_candidate.mal_anime_id, 38268)
        candidates_by_id = {candidate.mal_anime_id: candidate for candidate in result.candidates}
        self.assertIn(30954, candidates_by_id)
        self.assertIn("ona_penalty_for_explicit_season_context", candidates_by_id[30954].match_reasons)
        self.assertGreater(candidates_by_id[38268].score, candidates_by_id[30954].score)

    def test_build_search_queries_combines_generic_season_title_with_base_title(self) -> None:
        queries = build_search_queries(
            SeriesMappingInput(
                provider="crunchyroll",
                provider_series_id="series-123",
                title="Campfire Cooking in Another World with My Absurd Skill",
                season_title="Season 2",
                season_number=2,
            )
        )

        self.assertEqual(queries[0:2], [
            "Season 2",
            "Campfire Cooking in Another World with My Absurd Skill Season 2",
        ])
        self.assertIn("Campfire Cooking in Another World with My Absurd Skill 2nd Season", queries)
        self.assertIn("Campfire Cooking in Another World with My Absurd Skill 2", queries)
        self.assertIn("Campfire Cooking in Another World with My Absurd Skill II", queries)
        self.assertEqual(queries[-1], "Campfire Cooking in Another World with My Absurd Skill")

    def test_build_search_queries_normalizes_missing_spacing_in_installment_markers(self) -> None:
        queries = build_search_queries(
            SeriesMappingInput(
                provider="crunchyroll",
                provider_series_id="series-123",
                title="The Saint's Magic Power is Omnipotent",
                season_title="The Saint's Magic Power is Omnipotent Season2 (English Dub)",
                season_number=2,
            )
        )

        self.assertEqual(queries[0:2], [
            "The Saint's Magic Power is Omnipotent Season2 (English Dub)",
            "The Saint's Magic Power is Omnipotent Season 2",
        ])
        self.assertIn("The Saint's Magic Power is Omnipotent 2nd Season", queries)
        self.assertIn("The Saint's Magic Power is Omnipotent 2", queries)
        self.assertIn("The Saint's Magic Power is Omnipotent II", queries)
        self.assertEqual(queries[-1], "The Saint's Magic Power is Omnipotent")

    def test_build_search_queries_adds_franchise_specific_sequel_alias_for_explicit_later_season(self) -> None:
        queries = build_search_queries(
            SeriesMappingInput(
                provider="crunchyroll",
                provider_series_id="series-railgun-s",
                title="A Certain Scientific Railgun",
                season_title="A Certain Scientific Railgun Season 2 (English Dub)",
                season_number=2,
            )
        )

        self.assertIn("A Certain Scientific Railgun S", queries)
        self.assertLess(queries.index("A Certain Scientific Railgun Season 2"), queries.index("A Certain Scientific Railgun S"))
        self.assertLess(queries.index("A Certain Scientific Railgun S"), queries.index("A Certain Scientific Railgun"))

    def test_build_search_queries_infers_sequel_alias_from_season_title_when_metadata_season_missing(self) -> None:
        queries = build_search_queries(
            SeriesMappingInput(
                provider="crunchyroll",
                provider_series_id="series-railgun-s-no-metadata",
                title="A Certain Scientific Railgun",
                season_title="A Certain Scientific Railgun Season 2 (English Dub)",
                season_number=None,
            )
        )

        self.assertIn("A Certain Scientific Railgun Season 2", queries)
        self.assertIn("A Certain Scientific Railgun 2nd Season", queries)
        self.assertIn("A Certain Scientific Railgun S", queries)
        self.assertLess(queries.index("A Certain Scientific Railgun Season 2"), queries.index("A Certain Scientific Railgun S"))

    def test_build_search_queries_adds_punctuation_significant_franchise_specific_sequel_alias(self) -> None:
        queries = build_search_queries(
            SeriesMappingInput(
                provider="crunchyroll",
                provider_series_id="series-devil-part-timer-s2",
                title="The Devil is a Part-Timer!",
                season_title="The Devil is a Part-Timer! Season 2 (English Dub)",
                season_number=2,
            )
        )

        self.assertIn("The Devil is a Part-Timer!!", queries)
        self.assertLess(queries.index("The Devil is a Part-Timer! Season 2"), queries.index("The Devil is a Part-Timer!!"))
        self.assertLess(queries.index("The Devil is a Part-Timer!!"), queries.index("The Devil is a Part-Timer!"))

    def test_build_search_queries_adds_generic_stage_variants_for_later_season(self) -> None:
        queries = build_search_queries(
            SeriesMappingInput(
                provider="crunchyroll",
                provider_series_id="series-stage-query",
                title="Example Show",
                season_title="Example Show Season 2 (English Dub)",
                season_number=2,
            )
        )

        self.assertIn("Example Show Second Stage", queries)
        self.assertIn("Example Show 2nd Stage", queries)
        self.assertLess(queries.index("Example Show 2nd Season"), queries.index("Example Show Second Stage"))
        self.assertLess(queries.index("Example Show Second Stage"), queries.index("Example Show"))

    def test_build_search_queries_adds_generic_beat_variants_for_later_season(self) -> None:
        queries = build_search_queries(
            SeriesMappingInput(
                provider="crunchyroll",
                provider_series_id="series-beat-query",
                title="Example Show",
                season_title="Example Show Season 2 (English Dub)",
                season_number=2,
            )
        )

        self.assertIn("Example Show Second Beat", queries)
        self.assertIn("Example Show 2nd Beat", queries)
        self.assertLess(queries.index("Example Show 2nd Season"), queries.index("Example Show Second Beat"))
        self.assertLess(queries.index("Example Show Second Beat"), queries.index("Example Show"))

    def test_build_search_queries_strips_broadcast_and_uncensored_suffix_noise(self) -> None:
        queries = build_search_queries(
            SeriesMappingInput(
                provider="crunchyroll",
                provider_series_id="series-123",
                title="Harem in the Labyrinth of Another World",
                season_title="Harem in the Labyrinth of Another World - Broadcast Version (Uncensored)",
            )
        )

        self.assertEqual(
            queries[0:2],
            [
                "Harem in the Labyrinth of Another World - Broadcast Version (Uncensored)",
                "Harem in the Labyrinth of Another World",
            ],
        )
        self.assertEqual(queries[-1], "Harem in the Labyrinth of Another World")

    def test_map_series_boosts_parenthetical_english_alias_to_base_title_match(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 19685,
                                "title": "Kanojo ga Flag wo Oraretara",
                                "alternative_titles": {},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 13,
                            }
                        },
                        {
                            "node": {
                                "id": 24451,
                                "title": "Kanojo ga Flag wo Oraretara: Christmas? Sonna Mono ga Boku ni Tsuuyou Suru to Omou no ka?",
                                "alternative_titles": {},
                                "media_type": "ova",
                                "status": "finished_airing",
                                "num_episodes": 1,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-parenthetical-alias",
                        title="Kanojo ga Flag wo Oraretara (If Her Flag Breaks)",
                        season_title="Kanojo ga Flag wo Oraretara (If Her Flag Breaks)",
                        max_episode_number=13,
                        completed_episode_count=13,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertTrue(should_auto_approve_mapping(result))
        self.assertIsNotNone(result.chosen_candidate)
        self.assertEqual(result.chosen_candidate.mal_anime_id, 19685)
        self.assertIn("exact_base_title_after_subtitle_trim", result.rationale)

    def test_map_series_prefers_exact_specific_installment_over_base_title_tie(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            responses = {
                "Restaurant to Another World 2 (English Dub)": {
                    "data": [
                        {
                            "node": {
                                "id": 48804,
                                "title": "Isekai Shokudou 2",
                                "alternative_titles": {"en": "Restaurant to Another World 2"},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
                "Restaurant to Another World": {
                    "data": [
                        {
                            "node": {
                                "id": 34012,
                                "title": "Isekai Shokudou",
                                "alternative_titles": {"en": "Restaurant to Another World"},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            }

            with patch.object(MalClient, "search_anime", side_effect=lambda query, limit=5: responses.get(query, {"data": []})):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Restaurant to Another World",
                        season_title="Restaurant to Another World 2 (English Dub)",
                        season_number=2,
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertIsNotNone(result.chosen_candidate)
        self.assertEqual(result.chosen_candidate.mal_anime_id, 48804)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_uses_roman_query_variant_for_later_season_search(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            def fake_search(query: str, limit: int = 5) -> dict:
                if query == "Classroom of the Elite III":
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 51180,
                                    "title": "Youkoso Jitsuryoku Shijou Shugi no Kyoushitsu e 3rd Season",
                                    "alternative_titles": {"en": "Classroom of the Elite III"},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 13,
                                }
                            },
                            {
                                "node": {
                                    "id": 51096,
                                    "title": "Youkoso Jitsuryoku Shijou Shugi no Kyoushitsu e 2nd Season",
                                    "alternative_titles": {"en": "Classroom of the Elite II"},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 13,
                                }
                            },
                        ]
                    }
                return {"data": []}

            with patch.object(MalClient, "search_anime", side_effect=fake_search):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Classroom of the Elite",
                        season_title="Classroom of the Elite Season 3 (English Dub)",
                        season_number=3,
                        max_episode_number=13,
                        completed_episode_count=13,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertIsNotNone(result.chosen_candidate)
        self.assertEqual(result.chosen_candidate.mal_anime_id, 51180)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_prefixes_standalone_roman_season_title_without_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            seen_queries: list[str] = []

            def fake_search(query: str, limit: int = 5) -> dict:
                seen_queries.append(query)
                if query == "Classroom of the Elite III":
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 51180,
                                    "title": "Youkoso Jitsuryoku Shijou Shugi no Kyoushitsu e 3rd Season",
                                    "alternative_titles": {"en": "Classroom of the Elite III"},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 13,
                                }
                            },
                            {
                                "node": {
                                    "id": 51096,
                                    "title": "Youkoso Jitsuryoku Shijou Shugi no Kyoushitsu e 2nd Season",
                                    "alternative_titles": {"en": "Classroom of the Elite II"},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 13,
                                }
                            },
                        ]
                    }
                if query == "III":
                    return {"data": []}
                return {"data": []}

            with patch.object(MalClient, "search_anime", side_effect=fake_search):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-roman-standalone",
                        title="Classroom of the Elite",
                        season_title="III",
                        max_episode_number=13,
                        completed_episode_count=13,
                    ),
                )

        self.assertIn("Classroom of the Elite III", seen_queries)
        self.assertEqual(result.status, "exact")
        self.assertIsNotNone(result.chosen_candidate)
        self.assertEqual(result.chosen_candidate.mal_anime_id, 51180)
        self.assertIn("season_number_match=3", result.rationale)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_prefixes_standalone_numeric_season_title_without_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            seen_queries: list[str] = []

            def fake_search(query: str, limit: int = 5) -> dict:
                seen_queries.append(query)
                if query == "Restaurant to Another World 2":
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 48804,
                                    "title": "Isekai Shokudou 2",
                                    "alternative_titles": {"en": "Restaurant to Another World 2"},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 12,
                                }
                            },
                            {
                                "node": {
                                    "id": 34012,
                                    "title": "Isekai Shokudou",
                                    "alternative_titles": {"en": "Restaurant to Another World"},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 12,
                                }
                            },
                        ]
                    }
                if query == "2":
                    return {"data": []}
                return {"data": []}

            with patch.object(MalClient, "search_anime", side_effect=fake_search):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-numeric-standalone",
                        title="Restaurant to Another World",
                        season_title="2",
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertIn("Restaurant to Another World 2", seen_queries)
        self.assertEqual(result.status, "exact")
        self.assertIsNotNone(result.chosen_candidate)
        self.assertEqual(result.chosen_candidate.mal_anime_id, 48804)
        self.assertIn("season_number_match=2", result.rationale)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_expands_related_anime_to_recover_hidden_tv_sequel(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            def fake_search(query: str, limit: int = 5) -> dict:
                if query == "My Wife is the Student Council President+! (Uncensored)":
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 31980,
                                    "title": "Okusama ga Seitokaichou! Seitokaichou to Ofuro Asobi",
                                    "alternative_titles": {
                                        "synonyms": ["Okusama ga Seitokaichou! OVA"],
                                        "en": "My Wife is the Student Council President OVA",
                                        "ja": "",
                                    },
                                    "media_type": "ova",
                                    "status": "finished_airing",
                                    "num_episodes": 1,
                                }
                            },
                            {
                                "node": {
                                    "id": 5909,
                                    "title": "Seitokai no Ichizon",
                                    "alternative_titles": {},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 12,
                                }
                            },
                        ]
                    }
                return {"data": []}

            def fake_details(anime_id: int, *, fields: str = "") -> dict:
                if anime_id == 31980:
                    return {
                        "id": 31980,
                        "title": "Okusama ga Seitokaichou! Seitokaichou to Ofuro Asobi",
                        "alternative_titles": {
                            "synonyms": ["Okusama ga Seitokaichou! OVA"],
                            "en": "My Wife is the Student Council President OVA",
                            "ja": "",
                        },
                        "media_type": "ova",
                        "status": "finished_airing",
                        "num_episodes": 1,
                        "related_anime": [
                            {
                                "node": {"id": 28819, "title": "Okusama ga Seitokaichou!"},
                                "relation_type": "parent_story",
                                "relation_type_formatted": "Parent story",
                            }
                        ],
                    }
                if anime_id == 28819:
                    return {
                        "id": 28819,
                        "title": "Okusama ga Seitokaichou!",
                        "alternative_titles": {
                            "synonyms": ["Oku-sama ga Seito Kaichou!"],
                            "en": "My Wife is the Student Council President!",
                            "ja": "",
                        },
                        "media_type": "tv",
                        "status": "finished_airing",
                        "num_episodes": 12,
                        "related_anime": [
                            {
                                "node": {"id": 32603, "title": "Okusama ga Seitokaichou!+!"},
                                "relation_type": "sequel",
                                "relation_type_formatted": "Sequel",
                            }
                        ],
                    }
                if anime_id == 32603:
                    return {
                        "id": 32603,
                        "title": "Okusama ga Seitokaichou!+!",
                        "alternative_titles": {
                            "synonyms": [
                                "My Wife is the Student Council President 2nd Season",
                                "Okusama ga Seitokaichou! Plus",
                            ],
                            "en": "My Wife is the Student Council President!+",
                            "ja": "",
                        },
                        "media_type": "tv",
                        "status": "finished_airing",
                        "num_episodes": 12,
                        "related_anime": [],
                    }
                if anime_id == 5909:
                    return {
                        "id": 5909,
                        "title": "Seitokai no Ichizon",
                        "alternative_titles": {},
                        "media_type": "tv",
                        "status": "finished_airing",
                        "num_episodes": 12,
                        "related_anime": [],
                    }
                raise AssertionError(f"unexpected anime details lookup: {anime_id}")

            with patch.object(MalClient, "search_anime", side_effect=fake_search), patch.object(
                MalClient,
                "get_anime_details",
                side_effect=fake_details,
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-related-expansion",
                        title="My Wife is the Student Council President",
                        season_title="My Wife is the Student Council President+! (Uncensored)",
                        season_number=2,
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertIsNotNone(result.chosen_candidate)
        self.assertEqual(result.chosen_candidate.mal_anime_id, 32603)
        self.assertIn("related_anime_expansion", result.chosen_candidate.match_reasons)
        self.assertIn("installment_hint_match=plus", result.chosen_candidate.match_reasons)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_expands_related_anime_for_suffix_residue_without_installment_context(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            def fake_search(query: str, limit: int = 5) -> dict:
                if query == "Shuffle! (English Dub)":
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 1836,
                                    "title": "Shuffle! Memories",
                                    "alternative_titles": {},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 12,
                                }
                            }
                        ]
                    }
                return {"data": []}

            def fake_details(anime_id: int, *, fields: str = "") -> dict:
                if anime_id == 1836:
                    return {
                        "id": 1836,
                        "title": "Shuffle! Memories",
                        "alternative_titles": {},
                        "media_type": "tv",
                        "status": "finished_airing",
                        "num_episodes": 12,
                        "related_anime": [
                            {
                                "node": {"id": 79, "title": "Shuffle!"},
                                "relation_type": "prequel",
                                "relation_type_formatted": "Prequel",
                            }
                        ],
                    }
                if anime_id == 79:
                    return {
                        "id": 79,
                        "title": "Shuffle!",
                        "alternative_titles": {"en": "Shuffle!", "synonyms": [], "ja": ""},
                        "media_type": "tv",
                        "status": "finished_airing",
                        "num_episodes": 24,
                        "related_anime": [],
                    }
                raise AssertionError(f"unexpected anime details lookup: {anime_id}")

            with patch.object(MalClient, "search_anime", side_effect=fake_search), patch.object(
                MalClient,
                "get_anime_details",
                side_effect=fake_details,
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-shuffle",
                        title="Shuffle!",
                        season_title="Shuffle! (English Dub)",
                        season_number=1,
                        max_episode_number=8,
                        completed_episode_count=8,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertIsNotNone(result.chosen_candidate)
        self.assertEqual(result.chosen_candidate.mal_anime_id, 79)
        self.assertIn("related_anime_expansion", result.chosen_candidate.match_reasons)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_prioritizes_promising_relation_chains_before_low_value_siblings(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            def fake_search(query: str, limit: int = 5) -> dict:
                if query == "Seven Mortal Sins (English Dub)":
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 35418,
                                    "title": "Sin: Nanatsu no Taizai Zange-roku Specials",
                                    "alternative_titles": {"en": "Seven Mortal Sins Specials", "synonyms": [], "ja": ""},
                                    "media_type": "special",
                                    "status": "finished_airing",
                                    "num_episodes": 7,
                                }
                            },
                            {
                                "node": {
                                    "id": 23755,
                                    "title": "Nanatsu no Taizai",
                                    "alternative_titles": {"en": "The Seven Deadly Sins", "synonyms": [], "ja": ""},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 24,
                                }
                            },
                        ]
                    }
                return {"data": []}

            def fake_details(anime_id: int, *, fields: str = "") -> dict:
                if anime_id == 35418:
                    return {
                        "id": 35418,
                        "title": "Sin: Nanatsu no Taizai Zange-roku Specials",
                        "alternative_titles": {"en": "Seven Mortal Sins Specials", "synonyms": [], "ja": ""},
                        "media_type": "special",
                        "status": "finished_airing",
                        "num_episodes": 7,
                        "related_anime": [
                            {
                                "node": {"id": 35417, "title": "Sin: Nanatsu no Taizai Zange-roku"},
                                "relation_type": "prequel",
                                "relation_type_formatted": "Prequel",
                            }
                        ],
                    }
                if anime_id == 35417:
                    return {
                        "id": 35417,
                        "title": "Sin: Nanatsu no Taizai Zange-roku",
                        "alternative_titles": {"en": "", "synonyms": [], "ja": ""},
                        "media_type": "ona",
                        "status": "finished_airing",
                        "num_episodes": 12,
                        "related_anime": [
                            {
                                "node": {"id": 33834, "title": "Sin: Nanatsu no Taizai"},
                                "relation_type": "other",
                                "relation_type_formatted": "Other",
                            }
                        ],
                    }
                if anime_id == 33834:
                    return {
                        "id": 33834,
                        "title": "Sin: Nanatsu no Taizai",
                        "alternative_titles": {"en": "Seven Mortal Sins", "synonyms": [], "ja": ""},
                        "media_type": "tv",
                        "status": "finished_airing",
                        "num_episodes": 12,
                        "related_anime": [],
                    }
                if anime_id == 23755:
                    return {
                        "id": 23755,
                        "title": "Nanatsu no Taizai",
                        "alternative_titles": {"en": "The Seven Deadly Sins", "synonyms": [], "ja": ""},
                        "media_type": "tv",
                        "status": "finished_airing",
                        "num_episodes": 24,
                        "related_anime": [
                            {"node": {"id": 30347, "title": "Nanatsu no Taizai OVA"}},
                            {"node": {"id": 31722, "title": "Nanatsu no Taizai: Seisen no Shirushi"}},
                            {"node": {"id": 36923, "title": "Nanatsu no Taizai: Imashime no Fukkatsu Joshou"}},
                        ],
                    }
                if anime_id in {30347, 31722, 36923}:
                    return {
                        "id": anime_id,
                        "title": f"noise-{anime_id}",
                        "alternative_titles": {},
                        "media_type": "ova",
                        "status": "finished_airing",
                        "num_episodes": 1,
                        "related_anime": [],
                    }
                raise AssertionError(f"unexpected anime details lookup: {anime_id}")

            with patch.object(MalClient, "search_anime", side_effect=fake_search), patch.object(
                MalClient,
                "get_anime_details",
                side_effect=fake_details,
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-seven-mortal-sins",
                        title="Seven Mortal Sins",
                        season_title="Seven Mortal Sins (English Dub)",
                        season_number=1,
                        max_episode_number=7,
                        completed_episode_count=7,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertIsNotNone(result.chosen_candidate)
        self.assertEqual(result.chosen_candidate.mal_anime_id, 33834)
        self.assertIn("related_anime_expansion", result.chosen_candidate.match_reasons)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_does_not_expand_relations_for_plain_season_one_tv_match(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 37555,
                                "title": "Hangyakusei Million Arthur",
                                "alternative_titles": {"en": "Million Arthur", "synonyms": [], "ja": ""},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 10,
                            }
                        }
                    ]
                },
            ), patch.object(MalClient, "get_anime_details", side_effect=AssertionError("relation expansion should not run")):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-million-arthur",
                        title="Million Arthur",
                        season_title="Million Arthur Season 1 (English Dub)",
                        season_number=1,
                        max_episode_number=7,
                        completed_episode_count=7,
                    ),
                )

        self.assertIsNotNone(result.chosen_candidate)
        self.assertEqual(result.chosen_candidate.mal_anime_id, 37555)
        self.assertNotIn("related_anime_expansion", result.chosen_candidate.match_reasons)

    def test_map_series_classifies_exact_match_conservatively(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 42,
                                "title": "Attack on Titan Final Season",
                                "alternative_titles": {"synonyms": ["Attack on Titan Final Season (English Dub)"]},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 16,
                            }
                        },
                        {"node": {"id": 99, "title": "Random Other Show", "alternative_titles": {}, "media_type": "tv"}},
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Attack on Titan",
                        season_title="Attack on Titan Final Season (English Dub)",
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertIsNotNone(result.chosen_candidate)
        self.assertEqual(result.chosen_candidate.mal_anime_id, 42)

    def test_map_series_uses_season_and_episode_evidence_to_avoid_wrong_sequel(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 100,
                                "title": "Example Show Season 1",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 200,
                                "title": "Example Show Season 2",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Example Show",
                        season_title="Example Show Season 2",
                        season_number=2,
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertIsNotNone(result.chosen_candidate)
        self.assertEqual(result.chosen_candidate.mal_anime_id, 200)
        self.assertTrue(any("season_number_match=2" == reason for reason in result.rationale))

    def test_map_series_penalizes_candidate_with_extra_installment_hint_when_provider_has_none(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 666,
                                "title": "JoJo no Kimyou na Bouken",
                                "alternative_titles": {"en": "JoJo's Bizarre Adventure"},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 26,
                            }
                        },
                        {
                            "node": {
                                "id": 20899,
                                "title": "JoJo no Kimyou na Bouken Part 3: Stardust Crusaders",
                                "alternative_titles": {"en": "JoJo's Bizarre Adventure: Stardust Crusaders"},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 24,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="JoJo's Bizarre Adventure",
                        season_title="JoJo's Bizarre Adventure",
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertTrue(should_auto_approve_mapping(result))
        self.assertEqual(result.chosen_candidate.mal_anime_id, 666)
        self.assertIn("candidate_extra_installment_hint", result.candidates[1].match_reasons)

    def test_should_auto_approve_exact_unique_match(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 200,
                                "title": "Example Show Season 2",
                                "alternative_titles": {"synonyms": ["Example Show Season 2 (English Dub)"]},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 100,
                                "title": "Different Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Example Show",
                        season_title="Example Show Season 2 (English Dub)",
                        season_number=2,
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_penalizes_auxiliary_candidates_even_with_exact_title(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 28677,
                                "title": "Yamada-kun to 7-nin no Majo",
                                "alternative_titles": {"en": "Yamada-kun and the Seven Witches"},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 20359,
                                "title": "Yamada-kun to 7-nin no Majo PV",
                                "alternative_titles": {"en": "Yamada-kun and the Seven Witches"},
                                "media_type": "special",
                                "status": "finished_airing",
                                "num_episodes": 1,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Yamada-kun and the Seven Witches",
                        season_title="Yamada-kun and the Seven Witches",
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertTrue(should_auto_approve_mapping(result))
        self.assertEqual(result.chosen_candidate.mal_anime_id, 28677)
        self.assertIn("candidate_auxiliary_content=pv", result.candidates[1].match_reasons)

    def test_map_series_promotes_exact_tv_match_over_near_single_episode_ova_review_noise(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 28677,
                                "title": "Yamada-kun to 7-nin no Majo",
                                "alternative_titles": {"en": "Yamada-kun and the Seven Witches"},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 24627,
                                "title": "Yamada-kun to 7-nin no Majo: Mou Hitotsu no Suzaku-sai",
                                "alternative_titles": {"en": "Yamada-kun and the Seven Witches: Another Suzaku Festival"},
                                "media_type": "ova",
                                "status": "finished_airing",
                                "num_episodes": 1,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-ova-noise",
                        title="Yamada-kun and the Seven Witches",
                        season_title="Yamada-kun and the Seven Witches",
                        completed_episode_count=12,
                        max_episode_number=12,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertTrue(should_auto_approve_mapping(result))
        self.assertEqual(result.chosen_candidate.mal_anime_id, 28677)
        self.assertIn("substring_title_match", result.candidates[1].match_reasons)
        self.assertIn("episode_evidence_exceeds_candidate_count=12>1", result.candidates[1].match_reasons)

    def test_map_series_promotes_exact_tv_match_over_near_extra_suffix_franchise_entry_without_episode_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 28677,
                                "title": "Yamada-kun to 7-nin no Majo",
                                "alternative_titles": {"en": "Yamada-kun and the Seven Witches"},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": None,
                            }
                        },
                        {
                            "node": {
                                "id": 24627,
                                "title": "Yamada-kun to 7-nin no Majo: Mou Hitotsu no Suzaku-sai",
                                "alternative_titles": {"en": "Yamada-kun and the Seven Witches: Another Suzaku Festival"},
                                "media_type": "ova",
                                "status": "finished_airing",
                                "num_episodes": None,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-ova-suffix-noise",
                        title="Yamada-kun and the Seven Witches",
                        season_title="Yamada-kun and the Seven Witches",
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertTrue(should_auto_approve_mapping(result))
        self.assertEqual(result.chosen_candidate.mal_anime_id, 28677)
        self.assertIn("candidate_extra_title_suffix", result.candidates[1].match_reasons)

    def test_map_series_promotes_exact_base_series_over_sequel_suffix_variant(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 38297,
                                "title": "Maou-sama, Retry!",
                                "alternative_titles": {"en": "Demon Lord, Retry!"},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": None,
                            }
                        },
                        {
                            "node": {
                                "id": 56400,
                                "title": "Maou-sama, Retry! R",
                                "alternative_titles": {"en": "Demon Lord, Retry! R"},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": None,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-retry-base",
                        title="Demon Lord, Retry! (English Dub)",
                        season_title="Demon Lord, Retry! (English Dub)",
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertTrue(should_auto_approve_mapping(result))
        self.assertEqual(result.chosen_candidate.mal_anime_id, 38297)
        self.assertIn("candidate_extra_title_suffix", result.candidates[1].match_reasons)

    def test_map_series_promotes_exact_base_series_over_non_exact_tv_suffix_variants(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 6213,
                                "title": "Toaru Kagaku no Railgun",
                                "alternative_titles": {"en": "A Certain Scientific Railgun"},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 24,
                            }
                        },
                        {
                            "node": {
                                "id": 16049,
                                "title": "Toaru Kagaku no Railgun S",
                                "alternative_titles": {"en": "A Certain Scientific Railgun S"},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 24,
                            }
                        },
                        {
                            "node": {
                                "id": 38481,
                                "title": "Toaru Kagaku no Railgun T",
                                "alternative_titles": {"en": "A Certain Scientific Railgun T"},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 25,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-railgun-base",
                        title="A Certain Scientific Railgun (English Dub)",
                        season_title="A Certain Scientific Railgun (English Dub)",
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertTrue(should_auto_approve_mapping(result))
        self.assertEqual(result.chosen_candidate.mal_anime_id, 6213)
        self.assertIn("candidate_extra_title_suffix", result.candidates[1].match_reasons)
        self.assertNotIn("exact_normalized_title", result.candidates[1].match_reasons)

    def test_map_series_promotes_exact_tv_match_over_tv_special_when_title_only_differs_by_digit_spacing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 36023,
                                "title": "Persona 5 the Animation",
                                "alternative_titles": {},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": None,
                            }
                        },
                        {
                            "node": {
                                "id": 38431,
                                "title": "Persona 5 the Animation TV Specials",
                                "alternative_titles": {},
                                "media_type": "tv_special",
                                "status": "finished_airing",
                                "num_episodes": None,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-persona5",
                        title="PERSONA5 the Animation",
                        season_title="PERSONA5 the Animation",
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertTrue(should_auto_approve_mapping(result))
        self.assertEqual(result.chosen_candidate.mal_anime_id, 36023)
        self.assertIn("exact_normalized_title", result.chosen_candidate.match_reasons)
        self.assertIn("candidate_extra_title_suffix", result.candidates[1].match_reasons)

    def test_map_series_promotes_exact_tv_match_over_non_exact_ona_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 34257,
                                "title": "Cinderella Girls Gekijou",
                                "alternative_titles": {"en": "The iDOLM@STER CINDERELLA GIRLS Theater"},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 13,
                            }
                        },
                        {
                            "node": {
                                "id": 35346,
                                "title": "Cinderella Girls Gekijou: Kayou Cinderella Theater",
                                "alternative_titles": {},
                                "media_type": "ona",
                                "status": "finished_airing",
                                "num_episodes": 13,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-cingeki",
                        title="The iDOLM@STER CINDERELLA GIRLS Theater",
                        season_title="THE iDOLM@STER CINDERELLA GIRLS Theater 1st and 2nd Seasons (TV)",
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertTrue(should_auto_approve_mapping(result))
        self.assertEqual(result.chosen_candidate.mal_anime_id, 34257)
        self.assertEqual(result.candidates[1].media_type, "ona")
        self.assertNotIn("exact_normalized_title", result.candidates[1].match_reasons)

    def test_map_series_does_not_promote_exact_ova_match_over_base_tv_series(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 666,
                                "title": "JoJo no Kimyou na Bouken",
                                "alternative_titles": {"en": "JoJo's Bizarre Adventure"},
                                "media_type": "ova",
                                "status": "finished_airing",
                                "num_episodes": 6,
                            }
                        },
                        {
                            "node": {
                                "id": 14719,
                                "title": "JoJo no Kimyou na Bouken (TV)",
                                "alternative_titles": {},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 26,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-jojo-ova",
                        title="JoJo's Bizarre Adventure",
                        season_title="JoJo's Bizarre Adventure",
                        completed_episode_count=26,
                        max_episode_number=26,
                    ),
                )

        self.assertEqual(result.status, "ambiguous")
        self.assertFalse(should_auto_approve_mapping(result))
        self.assertEqual(result.chosen_candidate.mal_anime_id, 666)

    def test_should_not_auto_approve_when_season_evidence_conflicts(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 100,
                                "title": "Example Show Season 1",
                                "alternative_titles": {"synonyms": ["Example Show Season 2 (English Dub)"]},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 200,
                                "title": "Another Different Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Example Show",
                        season_title="Example Show Season 2 (English Dub)",
                        season_number=2,
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertFalse(should_auto_approve_mapping(result))

    def test_map_series_combined_generic_season_query_promotes_safe_exact_match(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            def fake_search(query: str, limit: int = 5) -> dict[str, object]:
                if query == "Campfire Cooking in Another World with My Absurd Skill Season 2":
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 500,
                                    "title": "Campfire Cooking in Another World with My Absurd Skill Season 2",
                                    "alternative_titles": {"synonyms": []},
                                    "media_type": "tv",
                                    "status": "currently_airing",
                                    "num_episodes": 12,
                                }
                            }
                        ]
                    }
                return {"data": []}

            with patch.object(MalClient, "search_anime", side_effect=fake_search):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Campfire Cooking in Another World with My Absurd Skill",
                        season_title="Season 2",
                        season_number=2,
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 500)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_uses_franchise_specific_sequel_alias_query_for_explicit_later_season(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            def fake_search(query: str, limit: int = 5) -> dict[str, object]:
                if query == "A Certain Scientific Railgun S":
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 16049,
                                    "title": "Toaru Kagaku no Railgun S",
                                    "alternative_titles": {"en": "A Certain Scientific Railgun S"},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 24,
                                }
                            }
                        ]
                    }
                return {"data": []}

            with patch.object(MalClient, "search_anime", side_effect=fake_search) as search_mock:
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-railgun-s",
                        title="A Certain Scientific Railgun",
                        season_title="A Certain Scientific Railgun Season 2 (English Dub)",
                        season_number=2,
                        max_episode_number=24,
                        completed_episode_count=24,
                    ),
                )

        attempted_queries = [call.args[0] for call in search_mock.call_args_list]
        self.assertIn("A Certain Scientific Railgun S", attempted_queries)
        self.assertEqual(result.status, "exact")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 16049)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_uses_franchise_specific_sequel_alias_query_when_season_metadata_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            def fake_search(query: str, limit: int = 5) -> dict[str, object]:
                if query == "A Certain Scientific Railgun S":
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 16049,
                                    "title": "Toaru Kagaku no Railgun S",
                                    "alternative_titles": {"en": "A Certain Scientific Railgun S"},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 24,
                                }
                            }
                        ]
                    }
                return {"data": []}

            with patch.object(MalClient, "search_anime", side_effect=fake_search) as search_mock:
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-railgun-s-no-metadata",
                        title="A Certain Scientific Railgun",
                        season_title="A Certain Scientific Railgun Season 2 (English Dub)",
                        season_number=None,
                        max_episode_number=24,
                        completed_episode_count=24,
                    ),
                )

        attempted_queries = [call.args[0] for call in search_mock.call_args_list]
        self.assertIn("A Certain Scientific Railgun S", attempted_queries)
        self.assertEqual(result.status, "exact")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 16049)
        self.assertIn("season_alias_query_match=2", result.rationale)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_infers_later_season_context_from_alias_labeled_season_title(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            def fake_search(query: str, limit: int = 5) -> dict[str, object]:
                if query == "Demon Lord, Retry! R":
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 53009,
                                    "title": "Maou-sama, Retry! R",
                                    "alternative_titles": {"en": "Demon Lord, Retry! R"},
                                    "media_type": "tv",
                                    "status": "currently_airing",
                                    "num_episodes": 12,
                                }
                            }
                        ]
                    }
                if query == "Demon Lord, Retry!":
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 38297,
                                    "title": "Maou-sama, Retry!",
                                    "alternative_titles": {"en": "Demon Lord, Retry!"},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 12,
                                }
                            }
                        ]
                    }
                return {"data": []}

            with patch.object(MalClient, "search_anime", side_effect=fake_search):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-demon-lord-retry-r-no-metadata",
                        title="Demon Lord, Retry!",
                        season_title="Demon Lord, Retry! R (English Dub)",
                        season_number=None,
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertIsNotNone(result.chosen_candidate)
        self.assertEqual(result.chosen_candidate.mal_anime_id, 53009)
        self.assertIn("season_number_match=2", result.rationale)
        self.assertIn("season_alias_query_match=2", result.rationale)
        self.assertIn("exact_later_installment_alignment", result.rationale)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_infers_later_season_context_from_punctuation_significant_alias_labeled_season_title(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            def fake_search(query: str, limit: int = 5) -> dict[str, object]:
                if query == "The Devil is a Part-Timer!!":
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 53200,
                                    "title": "Hataraku Maou-sama!!",
                                    "alternative_titles": {"en": "The Devil is a Part-Timer!!"},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 12,
                                }
                            }
                        ]
                    }
                if query == "The Devil is a Part-Timer!":
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 15809,
                                    "title": "Hataraku Maou-sama!",
                                    "alternative_titles": {"en": "The Devil is a Part-Timer!"},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 13,
                                }
                            }
                        ]
                    }
                return {"data": []}

            with patch.object(MalClient, "search_anime", side_effect=fake_search):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-devil-part-timer-bangbang-no-metadata",
                        title="The Devil is a Part-Timer!",
                        season_title="The Devil is a Part-Timer!! (English Dub)",
                        season_number=None,
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertIsNotNone(result.chosen_candidate)
        self.assertEqual(result.chosen_candidate.mal_anime_id, 53200)
        self.assertIn("season_number_match=2", result.rationale)
        self.assertIn("season_alias_query_match=2", result.rationale)
        self.assertIn("exact_later_installment_alignment", result.rationale)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_expands_relations_for_explicit_later_season_base_title_false_positive(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            def fake_search(query: str, limit: int = 5) -> dict[str, object]:
                if query in {
                    "Example Show Season 2",
                    "Example Show 2nd Season",
                    "Example Show 2",
                    "Example Show II",
                    "Example Show",
                }:
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 100,
                                    "title": "Example Show",
                                    "alternative_titles": {"en": "Example Show"},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 12,
                                }
                            }
                        ]
                    }
                return {"data": []}

            def fake_details(anime_id: int, *, fields: str = "") -> dict[str, object]:
                if anime_id == 100:
                    return {
                        "id": 100,
                        "title": "Example Show",
                        "alternative_titles": {"en": "Example Show"},
                        "media_type": "tv",
                        "status": "finished_airing",
                        "num_episodes": 12,
                        "related_anime": [
                            {
                                "node": {"id": 200, "title": "Example Show Second Stage"},
                                "relation_type": "sequel",
                                "relation_type_formatted": "Sequel",
                            }
                        ],
                    }
                if anime_id == 200:
                    return {
                        "id": 200,
                        "title": "Example Show Second Stage",
                        "alternative_titles": {
                            "synonyms": ["Example Show 2nd Season"],
                            "en": "Example Show Second Stage",
                        },
                        "media_type": "tv",
                        "status": "finished_airing",
                        "num_episodes": 12,
                        "related_anime": [],
                    }
                raise AssertionError(f"unexpected anime details lookup: {anime_id}")

            with patch.object(MalClient, "search_anime", side_effect=fake_search), patch.object(
                MalClient,
                "get_anime_details",
                side_effect=fake_details,
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-related-base-false-positive",
                        title="Example Show",
                        season_title="Example Show Season 2",
                        season_number=2,
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertIsNotNone(result.chosen_candidate)
        self.assertEqual(result.chosen_candidate.mal_anime_id, 200)
        self.assertIn("related_anime_expansion", result.chosen_candidate.match_reasons)
        self.assertIn("season_number_match=2", result.rationale)
        self.assertIn("exact_later_installment_alignment", result.rationale)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_requires_candidate_alias_match_before_alias_query_adds_later_season_confidence(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            def fake_search(query: str, limit: int = 5) -> dict[str, object]:
                if query in {
                    "Demon Lord, Retry! R",
                    "Demon Lord, Retry! Season 2",
                    "Demon Lord, Retry! 2nd Season",
                    "Demon Lord, Retry! 2",
                    "Demon Lord, Retry! II",
                    "Demon Lord, Retry!",
                }:
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 38297,
                                    "title": "Maou-sama, Retry!",
                                    "alternative_titles": {"en": "Demon Lord, Retry!"},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 12,
                                }
                            }
                        ]
                    }
                return {"data": []}

            def fake_details(anime_id: int, *, fields: str = "") -> dict[str, object]:
                if anime_id == 38297:
                    return {
                        "id": 38297,
                        "title": "Maou-sama, Retry!",
                        "alternative_titles": {"en": "Demon Lord, Retry!"},
                        "media_type": "tv",
                        "status": "finished_airing",
                        "num_episodes": 12,
                        "related_anime": [
                            {
                                "node": {"id": 53009, "title": "Maou-sama, Retry! R"},
                                "relation_type": "sequel",
                                "relation_type_formatted": "Sequel",
                            }
                        ],
                    }
                if anime_id == 53009:
                    return {
                        "id": 53009,
                        "title": "Maou-sama, Retry! R",
                        "alternative_titles": {"en": "Demon Lord, Retry! R"},
                        "media_type": "tv",
                        "status": "currently_airing",
                        "num_episodes": 12,
                        "related_anime": [],
                    }
                raise AssertionError(f"unexpected anime details lookup: {anime_id}")

            with patch.object(MalClient, "search_anime", side_effect=fake_search), patch.object(
                MalClient,
                "get_anime_details",
                side_effect=fake_details,
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-demon-lord-retry-related-sequel",
                        title="Demon Lord, Retry!",
                        season_title="Demon Lord, Retry! R (English Dub)",
                        season_number=None,
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertIsNotNone(result.chosen_candidate)
        self.assertEqual(result.chosen_candidate.mal_anime_id, 53009)
        self.assertIn("related_anime_expansion", result.chosen_candidate.match_reasons)
        self.assertIn("season_number_match=2", result.rationale)
        self.assertIn("exact_later_installment_alignment", result.rationale)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_requires_punctuation_significant_alias_match_before_alias_query_adds_later_season_confidence(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            def fake_search(query: str, limit: int = 5) -> dict[str, object]:
                if query in {
                    "The Devil is a Part-Timer!!",
                    "The Devil is a Part-Timer! Season 2",
                    "The Devil is a Part-Timer! 2nd Season",
                    "The Devil is a Part-Timer! 2",
                    "The Devil is a Part-Timer! II",
                    "The Devil is a Part-Timer!",
                }:
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 15809,
                                    "title": "Hataraku Maou-sama!",
                                    "alternative_titles": {"en": "The Devil is a Part-Timer!"},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 13,
                                }
                            }
                        ]
                    }
                return {"data": []}

            def fake_details(anime_id: int, *, fields: str = "") -> dict[str, object]:
                if anime_id == 15809:
                    return {
                        "id": 15809,
                        "title": "Hataraku Maou-sama!",
                        "alternative_titles": {"en": "The Devil is a Part-Timer!"},
                        "media_type": "tv",
                        "status": "finished_airing",
                        "num_episodes": 13,
                        "related_anime": [
                            {
                                "node": {"id": 53200, "title": "Hataraku Maou-sama!!"},
                                "relation_type": "sequel",
                                "relation_type_formatted": "Sequel",
                            }
                        ],
                    }
                if anime_id == 53200:
                    return {
                        "id": 53200,
                        "title": "Hataraku Maou-sama!!",
                        "alternative_titles": {"en": "The Devil is a Part-Timer!!"},
                        "media_type": "tv",
                        "status": "finished_airing",
                        "num_episodes": 12,
                        "related_anime": [],
                    }
                raise AssertionError(f"unexpected anime details lookup: {anime_id}")

            with patch.object(MalClient, "search_anime", side_effect=fake_search), patch.object(
                MalClient,
                "get_anime_details",
                side_effect=fake_details,
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-devil-part-timer-related-sequel",
                        title="The Devil is a Part-Timer!",
                        season_title="The Devil is a Part-Timer!! (English Dub)",
                        season_number=None,
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertIsNotNone(result.chosen_candidate)
        self.assertEqual(result.chosen_candidate.mal_anime_id, 53200)
        self.assertIn("related_anime_expansion", result.chosen_candidate.match_reasons)
        self.assertIn("season_number_match=2", result.rationale)
        self.assertIn("season_alias_query_match=2", result.rationale)
        self.assertIn("exact_later_installment_alignment", result.rationale)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_treats_inferred_later_season_context_as_specific_during_exact_classification(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            def fake_search(query: str, limit: int = 5) -> dict[str, object]:
                if query in {
                    "Example Show Season 2",
                    "Example Show 2nd Season",
                    "Example Show 2",
                    "Example Show II",
                }:
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 200,
                                    "title": "Example Show Second Stage",
                                    "alternative_titles": {
                                        "synonyms": ["Example Show 2nd Season"],
                                        "en": "Example Show Second Stage",
                                    },
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 12,
                                }
                            }
                        ]
                    }
                if query == "Example Show":
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 100,
                                    "title": "Example Show",
                                    "alternative_titles": {"en": "Example Show"},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 12,
                                }
                            }
                        ]
                    }
                return {"data": []}

            with patch.object(MalClient, "search_anime", side_effect=fake_search):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-inferred-season-classification",
                        title="Example Show",
                        season_title="Example Show Season 2",
                        season_number=None,
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertIsNotNone(result.chosen_candidate)
        self.assertEqual(result.chosen_candidate.mal_anime_id, 200)
        self.assertIn("season_number_match=2", result.rationale)
        self.assertIn("exact_later_installment_alignment", result.rationale)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_uses_generic_stage_query_for_later_season_without_alias_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            def fake_search(query: str, limit: int = 5) -> dict[str, object]:
                if query == "Example Show Second Stage":
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 220,
                                    "title": "Example Show Second Stage",
                                    "alternative_titles": {"en": "Example Show Second Stage"},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 12,
                                }
                            }
                        ]
                    }
                if query == "Example Show":
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 100,
                                    "title": "Example Show",
                                    "alternative_titles": {"en": "Example Show"},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 12,
                                }
                            }
                        ]
                    }
                return {"data": []}

            with patch.object(MalClient, "search_anime", side_effect=fake_search):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-generic-stage-query",
                        title="Example Show",
                        season_title="Example Show Season 2 (English Dub)",
                        season_number=2,
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertIsNotNone(result.chosen_candidate)
        self.assertEqual(result.chosen_candidate.mal_anime_id, 220)
        self.assertEqual(result.chosen_candidate.matched_query, "Example Show Second Stage")
        self.assertIn("exact_normalized_title", result.rationale)
        self.assertIn("season_number_match=2", result.rationale)
        self.assertIn("exact_later_installment_alignment", result.rationale)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_uses_generic_beat_query_for_later_season_without_alias_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            def fake_search(query: str, limit: int = 5) -> dict[str, object]:
                if query == "Example Show Second Beat":
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 221,
                                    "title": "Example Show Second Beat",
                                    "alternative_titles": {"en": "Example Show Second Beat"},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 12,
                                }
                            }
                        ]
                    }
                if query == "Example Show":
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 100,
                                    "title": "Example Show",
                                    "alternative_titles": {"en": "Example Show"},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 12,
                                }
                            }
                        ]
                    }
                return {"data": []}

            with patch.object(MalClient, "search_anime", side_effect=fake_search):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-generic-beat-query",
                        title="Example Show",
                        season_title="Example Show Season 2 (English Dub)",
                        season_number=2,
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertIsNotNone(result.chosen_candidate)
        self.assertEqual(result.chosen_candidate.mal_anime_id, 221)
        self.assertEqual(result.chosen_candidate.matched_query, "Example Show Second Beat")
        self.assertIn("exact_normalized_title", result.rationale)
        self.assertIn("season_number_match=2", result.rationale)
        self.assertIn("exact_later_installment_alignment", result.rationale)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_prefers_whole_season_over_split_part_when_provider_only_signals_season(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            def fake_search(query: str, limit: int = 5) -> dict[str, object]:
                if query in {
                    "Example Show II",
                    "Example Show Season 2",
                    "Example Show 2nd Season",
                    "Example Show 2",
                }:
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 200,
                                    "title": "Example Show II",
                                    "alternative_titles": {"en": "Example Show II"},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 12,
                                }
                            },
                            {
                                "node": {
                                    "id": 201,
                                    "title": "Example Show II Part 2",
                                    "alternative_titles": {"en": "Example Show II Part 2"},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 12,
                                }
                            },
                        ]
                    }
                if query == "Example Show":
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 100,
                                    "title": "Example Show",
                                    "alternative_titles": {"en": "Example Show"},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 12,
                                }
                            }
                        ]
                    }
                return {"data": []}

            with patch.object(MalClient, "search_anime", side_effect=fake_search):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-example-show-ii-whole-season",
                        title="Example Show",
                        season_title="Example Show II",
                        season_number=2,
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertIsNotNone(result.chosen_candidate)
        self.assertEqual(result.chosen_candidate.mal_anime_id, 200)
        self.assertEqual(result.candidates[0].mal_anime_id, 200)
        self.assertEqual(result.candidates[1].mal_anime_id, 201)
        self.assertGreater(result.candidates[0].score, result.candidates[1].score)
        self.assertIn("candidate_extra_part_hint=part:2", result.candidates[1].match_reasons)
        self.assertIn("candidate_extra_split_hint=split:2", result.candidates[1].match_reasons)
        self.assertIn("exact_later_installment_alignment", result.rationale)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_uses_roman_installment_hint_to_break_tie(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 300,
                                "title": "A Certain Magical Index II",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 24,
                            }
                        },
                        {
                            "node": {
                                "id": 400,
                                "title": "A Certain Magical Index III",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 26,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="A Certain Magical Index",
                        season_title="A Certain Magical Index III (English Dub)",
                        season_number=3,
                        max_episode_number=26,
                        completed_episode_count=26,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 400)
        self.assertIn("roman_installment_match=roman:3", result.rationale)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_build_search_queries_combines_generic_cour_title_with_base_title(self) -> None:
        queries = build_search_queries(
            SeriesMappingInput(
                provider="crunchyroll",
                provider_series_id="series-123",
                title="Example Show",
                season_title="2nd Cour",
                season_number=1,
            )
        )

        self.assertEqual(
            queries,
            [
                "2nd Cour",
                "Example Show 2nd Cour",
                "Example Show",
            ],
        )

    def test_map_series_uses_split_installment_match_for_part_vs_cour(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 500,
                                "title": "Example Show Final Season Part 1",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 600,
                                "title": "Example Show The Final Season 2nd Cour",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Example Show",
                        season_title="Example Show Final Season Part 2 (English Dub)",
                        season_number=4,
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertEqual(result.chosen_candidate.mal_anime_id, 600)
        self.assertIn("split_installment_match=split:2", result.rationale)

    def test_map_series_softens_aggregate_episode_numbering_when_installment_and_completion_evidence_align(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 700,
                                "title": "Example Show Season 2",
                                "alternative_titles": {"synonyms": ["Example Show Season 2 (English Dub)"]},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 13,
                            }
                        },
                        {
                            "node": {
                                "id": 600,
                                "title": "Example Show Season 1",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Example Show",
                        season_title="Example Show Season 2 (English Dub)",
                        season_number=2,
                        max_episode_number=25,
                        completed_episode_count=13,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 700)
        self.assertTrue(any(reason == "aggregated_episode_numbering_suspected=25>13" for reason in result.rationale))
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_prefers_part_two_candidate_for_aggregated_second_season(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 59193,
                                "title": "Mushoku Tensei III: Isekai Ittara Honki Dasu",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "currently_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 45576,
                                "title": "Mushoku Tensei: Isekai Ittara Honki Dasu Part 2",
                                "alternative_titles": {"synonyms": ["Mushoku Tensei: Jobless Reincarnation Part 2"]},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 39535,
                                "title": "Mushoku Tensei: Isekai Ittara Honki Dasu",
                                "alternative_titles": {"synonyms": ["Mushoku Tensei: Jobless Reincarnation"]},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 11,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Mushoku Tensei: Jobless Reincarnation",
                        season_title="Mushoku Tensei: Jobless Reincarnation Season 2",
                        season_number=2,
                        max_episode_number=24,
                        completed_episode_count=12,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 45576)
        self.assertIn("season_to_split_match=part:2,split:2", result.rationale)
        self.assertTrue(any(reason == "aggregated_episode_numbering_suspected=24>12" for reason in result.rationale))
        self.assertNotIn("episode_evidence_exceeds_candidate_count=24>12", result.rationale)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_surfaces_split_part_companion_for_aggregate_season_shell(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 48417,
                                "title": "Maou Gakuin no Futekigousha II: Shijou Saikyou no Maou no Shiso, Tensei shite Shison-tachi no Gakkou e Kayou",
                                "alternative_titles": {"en": "The Misfit of Demon King Academy II"},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 48418,
                                "title": "Maou Gakuin no Futekigousha II: Shijou Saikyou no Maou no Shiso, Tensei shite Shison-tachi no Gakkou e Kayou Part 2",
                                "alternative_titles": {"en": "The Misfit of Demon King Academy II Part 2"},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 40496,
                                "title": "Maou Gakuin no Futekigousha: Shijou Saikyou no Maou no Shiso, Tensei shite Shison-tachi no Gakkou e Kayou",
                                "alternative_titles": {"en": "The Misfit of Demon King Academy"},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 13,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-misfit-s2",
                        title="The Misfit of Demon King Academy",
                        season_title="The Misfit of Demon King Academy II(English Dub)",
                        season_number=2,
                        max_episode_number=24,
                        completed_episode_count=24,
                    ),
                )

        self.assertEqual(result.status, "ambiguous")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 48417)
        self.assertIn("multi_entry_bundle_suspected=24<=12+12", result.rationale)
        self.assertEqual({48418}, {candidate.mal_anime_id for candidate in (result.bundle_companion_candidates or [])})
        self.assertTrue(result.is_deterministic_multi_entry_bundle())
        self.assertFalse(should_auto_approve_mapping(result))

    def test_deterministic_bundle_predicate_rejects_alias_only_bundle_without_installment_evidence(self) -> None:
        result = self._map_with_search_results(
            SeriesMappingInput(
                provider="crunchyroll",
                provider_series_id="series-alias-only-bundle",
                title="The Melancholy of Haruhi Suzumiya",
                season_title="The Melancholy of Haruhi Suzumiya (English Dub)",
                season_number=1,
                max_episode_number=28,
                completed_episode_count=28,
            ),
            [
                {
                    "id": 849,
                    "title": "Suzumiya Haruhi no Yuuutsu",
                    "alternative_titles": {"en": "The Melancholy of Haruhi Suzumiya", "synonyms": []},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 14,
                },
                {
                    "id": 4382,
                    "title": "Suzumiya Haruhi no Yuuutsu (2009)",
                    "alternative_titles": {"en": "The Melancholy of Haruhi Suzumiya", "synonyms": []},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 14,
                },
            ],
        )

        self.assertIn("multi_entry_bundle_suspected=28<=14+14", result.rationale)
        self.assertEqual({4382}, {candidate.mal_anime_id for candidate in (result.bundle_companion_candidates or [])})
        self.assertFalse(result.is_deterministic_multi_entry_bundle())

    def test_map_series_prefers_split_specific_candidate_over_broader_same_season_tie(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 53200,
                                "title": "Hataraku Maou-sama!! 2nd Season",
                                "alternative_titles": {"synonyms": ["The Devil is a Part-Timer! Season 2 Part 2 (English Dub)"]},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 48413,
                                "title": "Hataraku Maou-sama!!",
                                "alternative_titles": {"synonyms": ["The Devil is a Part-Timer! Season 2 (English Dub)"]},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 15809,
                                "title": "Hataraku Maou-sama!",
                                "alternative_titles": {"synonyms": ["The Devil is a Part-Timer! (English Dub)"]},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 13,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-devil-part-timer",
                        title="The Devil is a Part-Timer!",
                        season_title="The Devil is a Part-Timer! Season 2 Part 2 (English Dub)",
                        season_number=2,
                        max_episode_number=13,
                        completed_episode_count=13,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 53200)
        self.assertIn("season_to_split_match=part:2,split:2", result.rationale)
        second = result.candidates[1]
        self.assertEqual(second.mal_anime_id, 48413)
        self.assertIn("season_number_match=2", second.match_reasons)
        self.assertFalse(any(reason.startswith("season_to_split_match=") for reason in second.match_reasons))
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_softens_single_episode_overflow_when_later_season_hint_is_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 44037,
                                "title": "Shin no Nakama ja Nai to Yuusha no Party wo Oidasareta node, Henkyou de Slow Life suru Koto ni Shimashita",
                                "alternative_titles": {
                                    "en": "Banished from the Hero's Party, I Decided to Live a Quiet Life in the Countryside"
                                },
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 13,
                            }
                        },
                        {
                            "node": {
                                "id": 53488,
                                "title": "Shin no Nakama ja Nai to Yuusha no Party wo Oidasareta node, Henkyou de Slow Life suru Koto ni Shimashita 2nd",
                                "alternative_titles": {
                                    "en": "Banished from the Hero's Party, I Decided to Live a Quiet Life in the Countryside Season 2"
                                },
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Banished from the Hero's Party, I Decided to Live a Quiet Life in the Countryside",
                        season_title="Banished from the Hero's Party, I Decided to Live a Quiet Life in the Countryside Season2 (English Dub)",
                        season_number=2,
                        max_episode_number=13,
                        completed_episode_count=13,
                        max_completed_episode_number=13,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 53488)
        self.assertIn("season_number_match=2", result.rationale)
        self.assertTrue(any(reason == "minor_episode_overflow_suspected=13>12" for reason in result.rationale))
        self.assertNotIn("episode_evidence_exceeds_candidate_count=13>12", result.rationale)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_softens_single_episode_overflow_for_exact_base_title_match(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 6880,
                                "title": "Deadman Wonderland",
                                "alternative_titles": {"en": "Deadman Wonderland", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 10372,
                                "title": "Deadman Wonderland: Akai Knife Tsukai",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "ova",
                                "status": "finished_airing",
                                "num_episodes": 1,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Deadman Wonderland",
                        season_title="Deadman Wonderland (English Dub)",
                        season_number=1,
                        max_episode_number=13,
                        completed_episode_count=13,
                        max_completed_episode_number=13,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 6880)
        self.assertIn("exact_normalized_title", result.rationale)
        self.assertIn("minor_episode_overflow_suspected=13>12", result.rationale)
        self.assertNotIn("episode_evidence_exceeds_candidate_count=13>12", result.rationale)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_penalizes_base_installment_candidate_when_provider_explicitly_targets_later_season(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 710,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 711,
                                "title": "Example Show 2nd Season",
                                "alternative_titles": {"en": "Example Show Season 2", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Example Show",
                        season_title="Example Show Season 2 (English Dub)",
                        season_number=2,
                        max_episode_number=12,
                        completed_episode_count=12,
                        max_completed_episode_number=12,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 711)
        self.assertIn("season_number_match=2", result.rationale)
        weaker = next(candidate for candidate in result.candidates if candidate.mal_anime_id == 710)
        self.assertIn("base_installment_penalty_for_explicit_later_season", weaker.match_reasons)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_penalizes_single_special_when_provider_looks_like_multi_episode_main_series(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 710,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 711,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "special",
                                "status": "finished_airing",
                                "num_episodes": 1,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Example Show",
                        season_title="Example Show",
                        season_number=1,
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertEqual(result.chosen_candidate.mal_anime_id, 710)
        self.assertIn("special_penalty_for_multi_episode_series", result.candidates[1].match_reasons)

    def test_map_series_prefers_title_season_hint_when_provider_metadata_is_noisy(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 700,
                                "title": "Example Show Season 2",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 800,
                                "title": "Example Show Season 3",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Example Show",
                        season_title="Example Show Season 2",
                        season_number=3,
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertEqual(result.chosen_candidate.mal_anime_id, 700)
        self.assertIn("provider_season_metadata_conflict=metadata:3;title:2", result.rationale)

    def test_map_series_does_not_penalize_exact_movie_title_inside_collection(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 900,
                                "title": "Dragon Ball Super: Super Hero",
                                "alternative_titles": {"synonyms": ["Dragon Ball Super: Super Hero (English Dub)"]},
                                "media_type": "movie",
                                "status": "finished_airing",
                                "num_episodes": 1,
                            }
                        }
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Dragon Ball Movies",
                        season_title="Dragon Ball Super: Super Hero (English Dub)",
                        season_number=2115,
                        max_episode_number=1,
                        completed_episode_count=1,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 900)
        self.assertIn("movie_type_allowed_for_exact_title", result.rationale)

    def test_map_series_prefers_tv_bundle_over_exact_movie_and_prologue_sidecars_for_multi_episode_series(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 6922,
                                "title": "Fate/stay night Movie: Unlimited Blade Works",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "movie",
                                "status": "finished_airing",
                                "num_episodes": 1,
                            }
                        },
                        {
                            "node": {
                                "id": 22297,
                                "title": "Fate/stay night: Unlimited Blade Works",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 27821,
                                "title": "Fate/stay night: Unlimited Blade Works Prologue",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv_special",
                                "status": "finished_airing",
                                "num_episodes": 1,
                            }
                        },
                        {
                            "node": {
                                "id": 28701,
                                "title": "Fate/stay night: Unlimited Blade Works 2nd Season",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 13,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-fate-ubw",
                        title="Fate/stay night [Unlimited Blade Works]",
                        season_title="Fate/stay night [Unlimited Blade Works] (English Dub)",
                        season_number=1,
                        max_episode_number=24,
                        completed_episode_count=24,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertEqual(22297, result.chosen_candidate.mal_anime_id)
        self.assertIn("multi_entry_bundle_suspected=24<=12+13", result.rationale)
        self.assertEqual({28701}, {candidate.mal_anime_id for candidate in (result.bundle_companion_candidates or [])})
        self.assertTrue(result.has_deterministic_aggregate_progress_classification())
        self.assertFalse(should_auto_approve_mapping(result))
        movie_candidate = next(candidate for candidate in result.candidates if candidate.mal_anime_id == 6922)
        prologue_candidate = next(candidate for candidate in result.candidates if candidate.mal_anime_id == 27821)
        self.assertIn("single_episode_movie_penalty_for_multi_episode_series", movie_candidate.match_reasons)
        self.assertIn("movie_penalty", movie_candidate.match_reasons)
        self.assertIn("single_episode_tv_special_penalty_for_multi_episode_series", prologue_candidate.match_reasons)
        self.assertLess(movie_candidate.score, result.chosen_candidate.score)
        self.assertLess(prologue_candidate.score, result.chosen_candidate.score)

    def test_map_series_uses_supplemental_title_candidate_for_unsearchable_exact_title(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(MalClient, "search_anime", return_value={"data": []}), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 40708,
                    "title": "Monster Musume no Oishasan",
                    "alternative_titles": {"en": "Monster Girl Doctor", "synonyms": [], "ja": "モンスター娘のお医者さん"},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 12,
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-monster-girl-doctor",
                        title="Monster Girl Doctor",
                        season_title="Monster Girl Doctor (English Dub)",
                        season_number=1,
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 40708)
        self.assertIn("supplemental_title_candidate", result.rationale)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_injects_verified_provider_identity_from_local_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            bootstrap_database(config.db_path)
            upsert_mal_anime_metadata(
                config.db_path,
                mal_anime_id=52736,
                title="Tensei Oujo to Tensai Reijou no Mahou Kakumei",
                title_english="The Magical Revolution of the Reincarnated Princess and the Genius Young Lady",
                title_japanese=None,
                alternative_titles=["The Magical Revolution of the Reincarnated Princess and the Genius Young Lady"],
                media_type="tv",
                status="finished_airing",
                num_episodes=12,
                mean=None,
                popularity=None,
                start_season={"year": 2023, "season": "winter"},
                raw={
                    "id": 52736,
                    "title": "Tensei Oujo to Tensai Reijou no Mahou Kakumei",
                    "alternative_titles": {
                        "en": "The Magical Revolution of the Reincarnated Princess and the Genius Young Lady",
                        "synonyms": [],
                    },
                },
            )
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 37430,
                                "title": "Tensei shitara Slime Datta Ken",
                                "alternative_titles": {"en": "That Time I Got Reincarnated as a Slime"},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 24,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                side_effect=AssertionError("local MAL metadata should cover verified identity injection"),
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="G5PHNM7J2",
                        title="The Magical Revolution of the Reincarnated Princess and the Genius Young Lady",
                        season_title="The Magical Revolution of the Reincarnated Princess and the Genius Young Lady",
                        season_number=1,
                        verified_mal_anime_id=52736,
                        verified_identity_kind="provider_title_search_exact",
                    ),
                )

        self.assertEqual(52736, result.chosen_candidate.mal_anime_id)
        self.assertEqual("exact", result.status)
        self.assertIn("verified_provider_identity=provider_title_search_exact", result.rationale)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_uses_local_metadata_exact_english_when_bounded_search_misses_long_title(self) -> None:
        title = "The Magical Revolution of the Reincarnated Princess and the Genius Young Lady"
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            bootstrap_database(config.db_path)
            upsert_mal_anime_metadata(
                config.db_path,
                mal_anime_id=52736,
                title="Tensei Oujo to Tensai Reijou no Mahou Kakumei",
                title_english=title,
                title_japanese=None,
                alternative_titles=[title],
                media_type="tv",
                status="finished_airing",
                num_episodes=12,
                mean=None,
                popularity=None,
                start_season={"year": 2023, "season": "winter"},
                raw={
                    "id": 52736,
                    "title": "Tensei Oujo to Tensai Reijou no Mahou Kakumei",
                    "alternative_titles": {"en": title, "synonyms": []},
                },
            )
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )
            attempted_queries: list[str] = []

            def fake_search(query: str, limit: int = 5, fields: str | None = None) -> dict:
                attempted_queries.append(query)
                self.assertLessEqual(len(query), 64)
                return {
                    "data": [
                        {
                            "node": {
                                "id": 37430,
                                "title": "Tensei shitara Slime Datta Ken",
                                "alternative_titles": {"en": "That Time I Got Reincarnated as a Slime", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 24,
                            }
                        }
                    ]
                }

            with patch.object(MalClient, "search_anime", side_effect=fake_search), patch.object(
                MalClient,
                "get_anime_details",
                side_effect=AssertionError("local MAL metadata should cover generic exact-title injection"),
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="G5PHNM7J2",
                        title=title,
                        season_title=title,
                        season_number=1,
                    ),
                )

        self.assertTrue(attempted_queries)
        self.assertTrue(all(len(query) <= 64 for query in attempted_queries))
        self.assertNotIn(title, attempted_queries)
        self.assertEqual("exact", result.status)
        self.assertEqual(52736, result.chosen_candidate.mal_anime_id)
        self.assertIn("local_mal_metadata_exact_title", result.rationale)
        self.assertIn("exact_mal_english_title", result.rationale)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_does_not_inject_duplicate_local_metadata_exact_titles(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            bootstrap_database(config.db_path)
            for anime_id, canonical_title, english_title in (
                (900001, "Example Local Show", "Example Local Show"),
                (900002, "Example Local Show: Side Story", "Example Local Show"),
            ):
                upsert_mal_anime_metadata(
                    config.db_path,
                    mal_anime_id=anime_id,
                    title=canonical_title,
                    title_english=english_title,
                    title_japanese=None,
                    alternative_titles=[english_title],
                    media_type="tv",
                    status="finished_airing",
                    num_episodes=12,
                    mean=None,
                    popularity=None,
                    start_season={"year": 2024, "season": "spring"},
                    raw={"id": anime_id, "title": canonical_title, "alternative_titles": {"en": english_title, "synonyms": []}},
                )
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(MalClient, "search_anime", return_value={"data": []}):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="duplicate-local-exact",
                        title="Example Local Show",
                        season_title="Example Local Show",
                    ),
                )

        self.assertEqual("no_candidates", result.status)
        self.assertIsNone(result.chosen_candidate)
        self.assertFalse(should_auto_approve_mapping(result))
        self.assertNotIn("local_mal_metadata_exact_title", result.rationale)

    def test_map_series_does_not_inject_local_metadata_exact_title_with_provider_evidence_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            bootstrap_database(config.db_path)
            upsert_mal_anime_metadata(
                config.db_path,
                mal_anime_id=900003,
                title="Conflict Local Show",
                title_english="Conflict Local Show",
                title_japanese=None,
                alternative_titles=["Conflict Local Show"],
                media_type="tv",
                status="finished_airing",
                num_episodes=12,
                mean=None,
                popularity=None,
                start_season={"year": 2023, "season": "winter"},
                raw={"id": 900003, "title": "Conflict Local Show", "alternative_titles": {"en": "Conflict Local Show", "synonyms": []}},
            )
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(MalClient, "search_anime", return_value={"data": []}):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="conflict-local-exact",
                        title="Conflict Local Show",
                        season_title="Conflict Local Show",
                        provider_episode_count=24,
                        provider_start_year=2024,
                        provider_start_year_is_trustworthy=True,
                    ),
                )

        self.assertEqual("no_candidates", result.status)
        self.assertIsNone(result.chosen_candidate)
        self.assertFalse(should_auto_approve_mapping(result))
        self.assertNotIn("local_mal_metadata_exact_title", result.rationale)

    def test_map_series_uses_cache_only_local_exact_title_for_magical_revolution(self) -> None:
        title = "The Magical Revolution of the Reincarnated Princess and the Genius Young Lady"
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            bootstrap_database(config.db_path)
            upsert_mal_anime_detail_cache(
                config.db_path,
                mal_anime_id=52736,
                fields_key="alternative_titles,id,media_type,num_episodes,start_season,status,title",
                logic_version=MAL_DETAIL_CACHE_LOGIC_VERSION,
                response={
                    "id": 52736,
                    "title": "Tensei Oujo to Tensai Reijou no Mahou Kakumei",
                    "alternative_titles": {"en": title, "synonyms": []},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 12,
                    "start_season": {"year": 2023, "season": "winter"},
                },
                fetched_at="2026-07-26T17:00:00Z",
                expires_at="2999-01-01T00:00:00Z",
            )
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            weak_node = {
                "id": 37430,
                "title": "Tensei shitara Slime Datta Ken",
                "alternative_titles": {"en": "That Time I Got Reincarnated as a Slime", "synonyms": []},
                "media_type": "tv",
                "status": "finished_airing",
                "num_episodes": 24,
            }
            with patch.object(MalClient, "search_anime", return_value={"data": [{"node": weak_node}]}), patch.object(
                MalClient,
                "get_anime_details",
                side_effect=AssertionError("fresh detail cache should cover local exact-title injection"),
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="G5PHNM7J2",
                        title=title,
                        season_title=title,
                        provider_episode_count=12,
                        provider_start_year=2023,
                        provider_start_year_is_trustworthy=True,
                    ),
                )

        self.assertEqual("exact", result.status)
        self.assertEqual(52736, result.chosen_candidate.mal_anime_id)
        self.assertIn("local_mal_metadata_exact_title", result.rationale)
        self.assertIn("exact_mal_english_title", result.rationale)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_ignores_unusable_detail_cache_exact_title_rows(self) -> None:
        title = "Unusable Cached Exact Show"
        valid_node = {
            "title": title,
            "alternative_titles": {"en": title, "synonyms": []},
            "media_type": "tv",
            "status": "finished_airing",
            "num_episodes": 12,
            "start_season": {"year": 2024, "season": "spring"},
        }
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            bootstrap_database(config.db_path)
            fields_key = "alternative_titles,id,media_type,num_episodes,start_season,status,title"
            rows = (
                (900020, fields_key, MAL_DETAIL_CACHE_LOGIC_VERSION, "ok", {**valid_node, "id": 900020}, "2000-01-01T00:00:00Z"),
                (900021, fields_key, MAL_DETAIL_CACHE_LOGIC_VERSION, "failed", {**valid_node, "id": 900021}, "2999-01-01T00:00:00Z"),
                (900022, fields_key, MAL_DETAIL_CACHE_LOGIC_VERSION, "ok", "not-json", "2999-01-01T00:00:00Z"),
                (900023, fields_key, MAL_DETAIL_CACHE_LOGIC_VERSION, "ok", {"id": 900023, "title": title}, "2999-01-01T00:00:00Z"),
            )
            with connect(config.db_path) as conn:
                for anime_id, fields_key, logic_version, status, response, expires_at in rows:
                    response_json = response if isinstance(response, str) else json.dumps(response)
                    conn.execute(
                        """
                        INSERT INTO mal_anime_detail_cache (
                            mal_anime_id, fields_key, logic_version, status, response_json, fetched_at, expires_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (anime_id, fields_key, logic_version, status, response_json, "2026-07-26T17:00:00Z", expires_at),
                    )
                conn.commit()
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(MalClient, "search_anime", return_value={"data": []}):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="unusable-detail-cache-exact",
                        title=title,
                        season_title=title,
                    ),
                )

        self.assertEqual("no_candidates", result.status)
        self.assertIsNone(result.chosen_candidate)
        self.assertFalse(should_auto_approve_mapping(result))

    def test_map_series_dedupes_same_local_exact_id_across_metadata_and_detail_cache(self) -> None:
        title = "Duplicate Local Exact Show"
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            bootstrap_database(config.db_path)
            upsert_mal_anime_metadata(
                config.db_path,
                mal_anime_id=900010,
                title="Duplicate Local Exact Show",
                title_english=title,
                title_japanese=None,
                alternative_titles=[title],
                media_type="tv",
                status="finished_airing",
                num_episodes=12,
                mean=None,
                popularity=None,
                start_season={"year": 2024, "season": "spring"},
                raw={"id": 900010, "title": title, "alternative_titles": {"en": title, "synonyms": []}},
            )
            upsert_mal_anime_detail_cache(
                config.db_path,
                mal_anime_id=900010,
                fields_key="alternative_titles,id,media_type,num_episodes,start_season,status,title",
                logic_version=MAL_DETAIL_CACHE_LOGIC_VERSION,
                response={
                    "id": 900010,
                    "title": title,
                    "alternative_titles": {"en": title, "synonyms": []},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 12,
                    "start_season": {"year": 2024, "season": "spring"},
                },
                fetched_at="2026-07-26T17:00:00Z",
                expires_at="2999-01-01T00:00:00Z",
            )
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(MalClient, "search_anime", return_value={"data": []}):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="same-id-local-exact",
                        title=title,
                        season_title=title,
                    ),
                )

        self.assertEqual("exact", result.status)
        self.assertEqual(900010, result.chosen_candidate.mal_anime_id)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_does_not_inject_multiple_distinct_detail_cache_exact_ids(self) -> None:
        title = "Ambiguous Cached Exact Show"
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            bootstrap_database(config.db_path)
            for anime_id, canonical_title in (
                (900011, title),
                (900012, f"{title}: Side Story"),
            ):
                upsert_mal_anime_detail_cache(
                    config.db_path,
                    mal_anime_id=anime_id,
                    fields_key="alternative_titles,id,media_type,num_episodes,start_season,status,title",
                    logic_version=MAL_DETAIL_CACHE_LOGIC_VERSION,
                    response={
                        "id": anime_id,
                        "title": canonical_title,
                        "alternative_titles": {"en": title, "synonyms": []},
                        "media_type": "tv",
                        "status": "finished_airing",
                        "num_episodes": 12,
                        "start_season": {"year": 2024, "season": "spring"},
                    },
                    fetched_at="2026-07-26T17:00:00Z",
                    expires_at="2999-01-01T00:00:00Z",
                )
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            weak_node = {
                "id": 37430,
                "title": "Tensei shitara Slime Datta Ken",
                "alternative_titles": {"en": "That Time I Got Reincarnated as a Slime", "synonyms": []},
                "media_type": "tv",
                "status": "finished_airing",
                "num_episodes": 24,
            }
            with patch.object(MalClient, "search_anime", return_value={"data": [{"node": weak_node}]}):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="multiple-cache-exacts",
                        title=title,
                        season_title=title,
                    ),
                )

        self.assertEqual("weak", result.status)
        self.assertEqual(37430, result.chosen_candidate.mal_anime_id)
        self.assertFalse(should_auto_approve_mapping(result))

    def test_map_series_blocks_detail_cache_exact_title_on_episode_or_year_conflict(self) -> None:
        title = "Conflicting Cached Exact Show"
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            bootstrap_database(config.db_path)
            upsert_mal_anime_detail_cache(
                config.db_path,
                mal_anime_id=900013,
                fields_key="alternative_titles,id,media_type,num_episodes,start_season,status,title",
                logic_version=MAL_DETAIL_CACHE_LOGIC_VERSION,
                response={
                    "id": 900013,
                    "title": title,
                    "alternative_titles": {"en": title, "synonyms": []},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 12,
                    "start_season": {"year": 2023, "season": "winter"},
                },
                fetched_at="2026-07-26T17:00:00Z",
                expires_at="2999-01-01T00:00:00Z",
            )
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            conflicting_series = (
                SeriesMappingInput(
                    provider="crunchyroll",
                    provider_series_id="cache-episode-conflict",
                    title=title,
                    season_title=title,
                    provider_episode_count=24,
                ),
                SeriesMappingInput(
                    provider="crunchyroll",
                    provider_series_id="cache-year-conflict",
                    title=title,
                    season_title=title,
                    provider_start_year=2024,
                    provider_start_year_is_trustworthy=True,
                ),
            )
            with patch.object(MalClient, "search_anime", return_value={"data": []}):
                results = [map_series(client, series) for series in conflicting_series]

        self.assertEqual(["no_candidates", "no_candidates"], [result.status for result in results])
        self.assertTrue(all(result.chosen_candidate is None for result in results))
        self.assertTrue(all(not should_auto_approve_mapping(result) for result in results))

    def test_map_series_discovers_overlong_exact_title_using_bounded_queries(self) -> None:
        title = "The Magical Revolution of the Reincarnated Princess and the Genius Young Lady"
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )
            attempted_queries: list[str] = []

            def fake_search(query: str, limit: int = 5, fields: str | None = None) -> dict:
                attempted_queries.append(query)
                self.assertLessEqual(len(query), 64)
                if query == "Magical Revolution Reincarnated Princess Genius Young Lady":
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 52736,
                                    "title": "Tensei Oujo to Tensai Reijou no Mahou Kakumei",
                                    "alternative_titles": {"en": title, "synonyms": []},
                                    "media_type": "tv",
                                    "status": "finished_airing",
                                    "num_episodes": 12,
                                }
                            }
                        ]
                    }
                return {"data": []}

            with patch.object(MalClient, "search_anime", side_effect=fake_search):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="G5PHNM7J2",
                        title=title,
                        season_title=title,
                        season_number=1,
                    ),
                )

        self.assertTrue(attempted_queries)
        self.assertNotIn(title, attempted_queries)
        self.assertEqual("exact", result.status)
        self.assertEqual(52736, result.chosen_candidate.mal_anime_id)
        self.assertIn("exact_normalized_title", result.rationale)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_does_not_promote_overlong_title_truncation_match(self) -> None:
        title = "The Magical Revolution of the Reincarnated Princess and the Genius Young Lady"
        truncated_title = "The Magical Revolution of the Reincarnated Princess"
        result = self._map_with_search_results(
            SeriesMappingInput(
                provider="crunchyroll",
                provider_series_id="G5PHNM7J2",
                title=title,
                season_title=title,
                season_number=1,
            ),
            [
                {
                    "id": 999001,
                    "title": truncated_title,
                    "alternative_titles": {"en": truncated_title, "synonyms": []},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 12,
                }
            ],
        )

        self.assertNotEqual("exact", result.status)
        self.assertFalse(should_auto_approve_mapping(result))
        self.assertIn("candidate_missing_provider_title_suffix=and_the_genius_young_lady", result.rationale)

    def test_map_series_keeps_verified_gintama_aggregate_shell_unmapped(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )
            gintama_918 = {
                "id": 918,
                "title": "Gintama",
                "alternative_titles": {"en": "Gintama", "synonyms": []},
                "media_type": "tv",
                "status": "finished_airing",
                "num_episodes": 201,
            }

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {"node": gintama_918},
                        {
                            "node": {
                                "id": 28977,
                                "title": "Gintama°",
                                "alternative_titles": {"en": "Gintama Season 4", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 51,
                            }
                        },
                        {
                            "node": {
                                "id": 9969,
                                "title": "Gintama'",
                                "alternative_titles": {"en": "Gintama Season 2", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 51,
                            }
                        },
                    ]
                },
            ), patch.object(MalClient, "get_anime_details", return_value=gintama_918):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="GYQ4MKDZ6",
                        title="Gintama",
                        season_title="Gintama",
                        verified_mal_anime_id=918,
                        verified_identity_kind="provider_title_search_exact",
                        provider_episode_count=382,
                        provider_season_count=8,
                    ),
                )

        self.assertEqual("ambiguous", result.status)
        self.assertEqual(918, result.chosen_candidate.mal_anime_id)
        self.assertTrue(result.has_provider_aggregate_shell_protection())
        self.assertIn("provider_aggregate_shell_suspected=provider_episodes:382;candidate_episodes:201;provider_seasons:8", result.rationale)
        self.assertFalse(should_auto_approve_mapping(result))

    def test_map_series_keeps_verified_multi_child_franchise_shell_unmapped(self) -> None:
        identity_evidence = {
            "child_titles": [
                {"title": "Rascal Does Not Dream of Bunny Girl Senpai", "episode_count": 13},
                {"title": "Rascal Does Not Dream of a Dreaming Girl", "episode_count": 1},
                {"title": "Rascal Does Not Dream of a Sister Venturing Out", "episode_count": 1},
                {"title": "Rascal Does Not Dream of a Knapsack Kid", "episode_count": 1},
                {"title": "Rascal Does Not Dream of Santa Claus", "episode_count": 13},
            ],
            "parent_episode_count": 29,
            "parent_season_count": 5,
        }
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )
            bunny_girl = {
                "id": 37450,
                "title": "Seishun Buta Yarou wa Bunny Girl Senpai no Yume wo Minai",
                "alternative_titles": {"en": "Rascal Does Not Dream of Bunny Girl Senpai", "synonyms": []},
                "media_type": "tv",
                "status": "finished_airing",
                "num_episodes": 13,
            }

            with patch.object(MalClient, "search_anime", return_value={"data": [{"node": bunny_girl}]}), patch.object(
                MalClient,
                "get_anime_details",
                return_value=bunny_girl,
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="GYW4MG9G6",
                        title="Rascal Does Not Dream Series",
                        season_title="Rascal Does Not Dream Series",
                        verified_mal_anime_id=37450,
                        verified_identity_kind="provider_franchise_shell_child_match",
                        verified_identity_evidence=identity_evidence,
                        provider_episode_count=29,
                        provider_season_count=5,
                    ),
                )

        self.assertEqual("ambiguous", result.status)
        self.assertEqual(37450, result.chosen_candidate.mal_anime_id)
        self.assertTrue(result.has_provider_aggregate_shell_protection())
        self.assertIn(
            "provider_franchise_shell_child_match_non_actionable=children:5;provider_episodes:29;candidate_episodes:13;child_episodes:29;provider_seasons:5",
            result.rationale,
        )
        self.assertFalse(should_auto_approve_mapping(result))

    def test_map_series_preserves_single_child_verified_franchise_identity(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )
            node = {
                "id": 123456,
                "title": "Example Single Child Show",
                "alternative_titles": {"en": "Example Single Child Show", "synonyms": []},
                "media_type": "tv",
                "status": "finished_airing",
                "num_episodes": 12,
            }
            with patch.object(MalClient, "search_anime", return_value={"data": [{"node": node}]}), patch.object(
                MalClient,
                "get_anime_details",
                return_value=node,
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="single-child-shell",
                        title="Example Single Child Show",
                        season_title="Example Single Child Show",
                        verified_mal_anime_id=123456,
                        verified_identity_kind="provider_franchise_shell_child_match",
                        verified_identity_evidence={
                            "child_titles": [{"title": "Example Single Child Show", "episode_count": 12}],
                            "parent_episode_count": 12,
                            "parent_season_count": 1,
                        },
                        provider_episode_count=12,
                        provider_season_count=1,
                    ),
                )

        self.assertEqual("exact", result.status)
        self.assertEqual(123456, result.chosen_candidate.mal_anime_id)
        self.assertFalse(result.has_provider_aggregate_shell_protection())
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_uses_supplemental_bundle_candidates_for_girls_bravo(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            details = {
                241: {
                    "id": 241,
                    "title": "Girls Bravo: First Season",
                    "alternative_titles": {"en": "Girls Bravo", "synonyms": [], "ja": "GIRLSブラボー first season"},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 11,
                },
                487: {
                    "id": 487,
                    "title": "Girls Bravo: Second Season",
                    "alternative_titles": {"en": "Girls Bravo: Second Season", "synonyms": [], "ja": "GIRLSブラボー second season"},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 13,
                },
            }

            with patch.object(MalClient, "search_anime", return_value={"data": []}), patch.object(
                MalClient,
                "get_anime_details",
                side_effect=lambda anime_id, fields=None: details[anime_id],
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-girls-bravo",
                        title="Girls Bravo",
                        season_title="Girls Bravo",
                        season_number=1,
                        max_episode_number=24,
                        completed_episode_count=24,
                    ),
                )

        self.assertEqual(result.status, "strong")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 241)
        self.assertEqual({487}, {candidate.mal_anime_id for candidate in (result.bundle_companion_candidates or [])})
        self.assertIn("multi_entry_bundle_suspected=24<=11+13", result.rationale)
        self.assertFalse(should_auto_approve_mapping(result))

    def test_map_series_auto_approves_exact_later_installment_when_base_title_is_aggregated_noise(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 48761,
                                "title": "Saihate no Paladin",
                                "alternative_titles": {"en": "The Faraway Paladin", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 50664,
                                "title": "Saihate no Paladin: Tetsusabi no Yama no Ou",
                                "alternative_titles": {"en": "The Faraway Paladin: The Lord of the Rust Mountains", "synonyms": ["Saihate no Paladin 2nd Season"]},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-faraway-paladin-rust-mountains",
                        title="The Faraway Paladin",
                        season_title="The Faraway Paladin The Lord Of The Rust Mountains (English Dub)",
                        season_number=2,
                        max_episode_number=13,
                        completed_episode_count=13,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 50664)
        self.assertIn("exact_provider_season_title", result.rationale)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_exact_specific_provider_title_beats_base_title_fragment(self) -> None:
        result = self._map_with_search_results(
            SeriesMappingInput(
                provider="crunchyroll",
                provider_series_id="G4PH0WXEM",
                title="Higurashi: When They Cry - GOU",
                season_title="Higurashi: When They Cry - GOU",
                provider_episode_count=39,
            ),
            [
                {
                    "id": 41006,
                    "title": "Higurashi no Naku Koro ni Gou",
                    "alternative_titles": {"en": "Higurashi: When They Cry - GOU"},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 24,
                    "start_season": {"season": "fall", "year": 2020},
                },
                {
                    "id": 934,
                    "title": "Higurashi no Naku Koro ni",
                    "alternative_titles": {"en": "Higurashi: When They Cry"},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 26,
                    "start_season": {"season": "spring", "year": 2006},
                },
            ],
        )

        self.assertEqual("exact", result.status)
        self.assertEqual(41006, result.chosen_candidate.mal_anime_id)
        self.assertTrue(should_auto_approve_mapping(result))
        self.assertTrue(
            any(
                reason.startswith("candidate_missing_provider_title_suffix=")
                for reason in result.candidates[1].match_reasons
            )
        )

    def test_map_series_uses_provider_episode_and_year_evidence_for_remake_tie(self) -> None:
        result = self._map_with_search_results(
            SeriesMappingInput(
                provider="crunchyroll",
                provider_series_id="GY3VKX1MR",
                title="Hunter x Hunter",
                season_title="Hunter x Hunter",
                provider_episode_count=148,
                provider_start_year=2011,
                provider_start_year_is_trustworthy=True,
            ),
            [
                {
                    "id": 136,
                    "title": "Hunter x Hunter",
                    "alternative_titles": {"en": "Hunter x Hunter", "synonyms": ["HxH"]},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 62,
                    "start_season": {"season": "fall", "year": 1999},
                },
                {
                    "id": 11061,
                    "title": "Hunter x Hunter (2011)",
                    "alternative_titles": {"en": "Hunter x Hunter", "synonyms": ["HxH (2011)"]},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 148,
                    "start_season": {"season": "fall", "year": 2011},
                },
            ],
        )

        self.assertEqual("exact", result.status)
        self.assertEqual(11061, result.chosen_candidate.mal_anime_id)
        self.assertIn("provider_episode_count_match=148", result.rationale)
        self.assertIn("provider_start_year_match=2011", result.rationale)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_uses_episode_count_without_trusting_catalog_launch_year_for_spice_and_wolf(self) -> None:
        evidence = extract_provider_mapping_evidence(
            {
                "raw": {
                    "series_metadata": {
                        "episode_count": 25,
                        "season_count": 2,
                        "series_launch_year": 2022,
                    }
                }
            }
        )
        self.assertEqual(25, evidence.episode_count)
        self.assertIsNone(evidence.start_year)

        result = self._map_with_search_results(
            SeriesMappingInput(
                provider="crunchyroll",
                provider_series_id="G6GG38246",
                title="Spice and Wolf",
                season_title="Spice and Wolf",
                provider_episode_count=evidence.episode_count,
                provider_start_year=evidence.start_year,
                provider_start_year_is_trustworthy=evidence.start_year_is_trustworthy,
            ),
            [
                {
                    "id": 51122,
                    "title": "Ookami to Koushinryou: Merchant Meets the Wise Wolf",
                    "alternative_titles": {
                        "en": "Spice and Wolf: Merchant Meets the Wise Wolf",
                        "synonyms": ["Spice and Wolf"],
                    },
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 25,
                    "start_season": {"season": "spring", "year": 2024},
                },
                {
                    "id": 2966,
                    "title": "Ookami to Koushinryou",
                    "alternative_titles": {"en": "Spice and Wolf", "synonyms": ["Ookami to Koushinryou"]},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 13,
                    "start_season": {"season": "winter", "year": 2008},
                },
            ],
        )

        self.assertEqual("exact", result.status)
        self.assertEqual(51122, result.chosen_candidate.mal_anime_id)
        self.assertIn("provider_episode_count_match=25", result.rationale)
        self.assertFalse(any(reason.startswith("provider_start_year_") for reason in result.rationale))
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_keeps_dororo_exact_title_remake_without_provider_evidence_human_gated(self) -> None:
        result = self._map_with_search_results(
            SeriesMappingInput(
                provider="hidive",
                provider_series_id="1181",
                title="Dororo",
                season_title="Dororo",
            ),
            self._dororo_search_nodes(),
        )

        self.assertEqual("ambiguous", result.status)
        self.assertFalse(should_auto_approve_mapping(result))
        self.assertEqual(37520, result.chosen_candidate.mal_anime_id)
        self.assertIn("exact_mal_primary_title", result.chosen_candidate.match_reasons)
        alias_candidate = next(candidate for candidate in result.candidates if candidate.mal_anime_id == 5760)
        self.assertIn("exact_mal_alternative_title", alias_candidate.match_reasons)
        self.assertIn("exact_mal_english_title", alias_candidate.match_reasons)
        self.assertEqual(1.0, result.candidates[0].score)
        self.assertEqual(1.0, result.candidates[1].score)
        self.assertIn("margin=0.000", result.rationale)

    def test_map_series_uses_dororo_episode_or_year_evidence_for_remake_tie(self) -> None:
        for label, provider_kwargs, expected_reason in (
            ("episode", {"provider_episode_count": 24}, "provider_episode_count_match=24"),
            (
                "year",
                {"provider_start_year": 2019, "provider_start_year_is_trustworthy": True},
                "provider_start_year_match=2019",
            ),
            (
                "episode_and_year",
                {"provider_episode_count": 24, "provider_start_year": 2019, "provider_start_year_is_trustworthy": True},
                "provider_start_year_match=2019",
            ),
        ):
            with self.subTest(label=label):
                result = self._map_with_search_results(
                    SeriesMappingInput(
                        provider="hidive",
                        provider_series_id="1181",
                        title="Dororo",
                        season_title="Dororo",
                        **provider_kwargs,
                    ),
                    copy.deepcopy(self._dororo_search_nodes()),
                )

            self.assertEqual("exact", result.status)
            self.assertEqual(37520, result.chosen_candidate.mal_anime_id)
            self.assertIn(expected_reason, result.rationale)
            self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_canonical_exact_tiebreak_does_not_create_score_margin(self) -> None:
        result = self._map_with_search_results(
            SeriesMappingInput(
                provider="hidive",
                provider_series_id="shared-title",
                title="Shared Title",
                season_title="Shared Title",
            ),
            [
                {
                    "id": 900010,
                    "title": "Shared Title Side Story",
                    "alternative_titles": {"en": "Shared Title", "synonyms": []},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 12,
                },
                {
                    "id": 900011,
                    "title": "Shared Title",
                    "alternative_titles": {"en": "Shared Title", "synonyms": []},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 12,
                },
            ],
        )

        self.assertEqual("ambiguous", result.status)
        self.assertFalse(should_auto_approve_mapping(result))
        self.assertEqual(900011, result.chosen_candidate.mal_anime_id)
        self.assertEqual(result.candidates[0].score, result.candidates[1].score)
        self.assertIn("margin=0.000", result.rationale)

    def test_build_mapping_review_replaces_unapproved_stale_mapping_for_exact_specific_title(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            bootstrap_database(config.db_path)
            _write_test_mal_secret_files(root)
            with connect(config.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO provider_series(provider, provider_series_id, title, season_title, season_number, raw_json, account_observed_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (
                        "hidive",
                        "1201",
                        "The Familiar of Zero F",
                        "The Familiar of Zero F",
                        None,
                        json.dumps({"title": "The Familiar of Zero F"}),
                    ),
                )
                conn.commit()
            upsert_series_mapping(
                config.db_path,
                provider="hidive",
                provider_series_id="1201",
                mal_anime_id=1195,
                confidence=0.92,
                mapping_source="reverse_provider_title_search",
                approved_by_user=False,
                notes="stale base-title provider search",
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 11319,
                                "title": "Zero no Tsukaima F",
                                "alternative_titles": {"en": "The Familiar of Zero F"},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 1195,
                                "title": "Zero no Tsukaima",
                                "alternative_titles": {"en": "The Familiar of Zero"},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 13,
                            }
                        },
                    ]
                },
            ):
                items = build_mapping_review(config, limit=5, mapping_limit=5, provider_series_ids=["1201"])
                persisted = get_series_mapping(config.db_path, "hidive", "1201")

        self.assertEqual(1, len(items))
        self.assertEqual("auto_approved", items[0].decision)
        self.assertEqual("approved", items[0].mapping_status)
        self.assertEqual(11319, items[0].suggested_mal_anime_id)
        self.assertIsNotNone(persisted)
        self.assertEqual(11319, persisted.mal_anime_id)
        self.assertTrue(persisted.approved_by_user)
        self.assertEqual("auto_exact", persisted.mapping_source)
        self.assertTrue(any("existing_mapping=1195:reverse_provider_title_search:approved=0" == reason for reason in items[0].reasons))

    def test_build_mapping_review_preserves_approved_mapping_even_when_new_exact_specific_title_exists(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            bootstrap_database(config.db_path)
            _write_test_mal_secret_files(root)
            with connect(config.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO provider_series(provider, provider_series_id, title, season_title, season_number, raw_json, account_observed_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (
                        "hidive",
                        "1201",
                        "The Familiar of Zero F",
                        "The Familiar of Zero F",
                        None,
                        json.dumps({"title": "The Familiar of Zero F"}),
                    ),
                )
                conn.commit()
            upsert_series_mapping(
                config.db_path,
                provider="hidive",
                provider_series_id="1201",
                mal_anime_id=1195,
                confidence=1.0,
                mapping_source="user_exact",
                approved_by_user=True,
                notes="manual mapping remains authoritative",
            )

            with patch.object(MalClient, "search_anime", side_effect=AssertionError("approved mapping should not be remapped")):
                items = build_mapping_review(config, limit=5, mapping_limit=5, provider_series_ids=["1201"])
                persisted = get_series_mapping(config.db_path, "hidive", "1201")

        self.assertEqual(1, len(items))
        self.assertEqual("preserved", items[0].decision)
        self.assertEqual(1195, items[0].suggested_mal_anime_id)
        self.assertEqual(1195, persisted.mal_anime_id)
        self.assertTrue(persisted.approved_by_user)

    def test_build_mapping_review_constructs_provider_episode_and_year_evidence_from_nested_raw(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            bootstrap_database(config.db_path)
            _write_test_mal_secret_files(root)
            with connect(config.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO provider_series(provider, provider_series_id, title, season_title, season_number, raw_json, account_observed_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (
                        "crunchyroll",
                        "GY3VKX1MR",
                        "Hunter x Hunter",
                        "Hunter x Hunter",
                        None,
                        json.dumps(
                            {
                                "raw": {
                                    "series_metadata": {
                                        "episode_count": 148,
                                        "season_count": 1,
                                        "series_launch_year": 2011,
                                    }
                                }
                            }
                        ),
                    ),
                )
                conn.commit()

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 136,
                                "title": "Hunter x Hunter",
                                "alternative_titles": {"en": "Hunter x Hunter", "synonyms": ["HxH"]},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 62,
                                "start_season": {"season": "fall", "year": 1999},
                            }
                        },
                        {
                            "node": {
                                "id": 11061,
                                "title": "Hunter x Hunter (2011)",
                                "alternative_titles": {"en": "Hunter x Hunter", "synonyms": ["HxH (2011)"]},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 148,
                                "start_season": {"season": "fall", "year": 2011},
                            }
                        },
                    ]
                },
            ):
                items = build_mapping_review(config, limit=5, mapping_limit=5, provider_series_ids=["GY3VKX1MR"])

        self.assertEqual("auto_approved", items[0].decision)
        self.assertEqual(11061, items[0].suggested_mal_anime_id)
        self.assertIn("provider_episode_count_match=148", items[0].reasons)
        self.assertIn("provider_start_year_match=2011", items[0].reasons)

    def test_build_mapping_review_uses_detail_cache_exact_title_when_metadata_absent(self) -> None:
        title = "The Magical Revolution of the Reincarnated Princess and the Genius Young Lady"
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            bootstrap_database(config.db_path)
            _write_test_mal_secret_files(root)
            with connect(config.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO provider_series(provider, provider_series_id, title, season_title, season_number, raw_json, account_observed_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (
                        "crunchyroll",
                        "G5PHNM7J2",
                        title,
                        title,
                        1,
                        json.dumps({"raw": {"series_metadata": {"episode_count": 12, "series_launch_year": 2023}}}),
                    ),
                )
                conn.commit()
            upsert_mal_anime_detail_cache(
                config.db_path,
                mal_anime_id=52736,
                fields_key="alternative_titles,id,media_type,num_episodes,start_season,status,title",
                logic_version=MAL_DETAIL_CACHE_LOGIC_VERSION,
                response={
                    "id": 52736,
                    "title": "Tensei Oujo to Tensai Reijou no Mahou Kakumei",
                    "alternative_titles": {"en": title, "synonyms": []},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 12,
                    "start_season": {"year": 2023, "season": "winter"},
                },
                fetched_at="2026-07-26T17:00:00Z",
                expires_at="2999-01-01T00:00:00Z",
            )
            weak_node = {
                "id": 37430,
                "title": "Tensei shitara Slime Datta Ken",
                "alternative_titles": {"en": "That Time I Got Reincarnated as a Slime", "synonyms": []},
                "media_type": "tv",
                "status": "finished_airing",
                "num_episodes": 24,
            }

            with patch.object(MalClient, "search_anime", return_value={"data": [{"node": weak_node}]}), patch.object(
                MalClient,
                "get_anime_details",
                side_effect=AssertionError("fresh detail cache should cover review exact-title injection"),
            ):
                items = build_mapping_review(config, limit=5, mapping_limit=5, provider_series_ids=["G5PHNM7J2"])

        self.assertEqual(1, len(items))
        self.assertEqual("auto_approved", items[0].decision)
        self.assertEqual("approved", items[0].mapping_status)
        self.assertEqual(52736, items[0].suggested_mal_anime_id)
        self.assertIn("local_mal_metadata_exact_title", items[0].reasons)
        self.assertIn("exact_mal_english_title", items[0].reasons)

    def test_map_series_auto_approves_exact_single_movie_feature(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / ".MAL-Updater" / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / ".MAL-Updater" / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / ".MAL-Updater" / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / ".MAL-Updater" / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 48561,
                                "title": "Jujutsu Kaisen 0 Movie",
                                "alternative_titles": {"en": "Jujutsu Kaisen 0", "synonyms": ["JJK 0"]},
                                "media_type": "movie",
                                "status": "finished_airing",
                                "num_episodes": 1,
                            }
                        },
                        {
                            "node": {
                                "id": 40748,
                                "title": "Jujutsu Kaisen",
                                "alternative_titles": {"en": "Jujutsu Kaisen", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 24,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-jjk-zero",
                        title="JUJUTSU KAISEN 0",
                        season_title="JUJUTSU KAISEN 0",
                        season_number=0,
                        max_episode_number=1,
                        completed_episode_count=1,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 48561)
        self.assertTrue(should_auto_approve_mapping(result))


class PersistedMappingTests(unittest.TestCase):
    def test_upsert_and_list_series_mappings(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            ingest_snapshot_payload(payload, config)

            created = upsert_series_mapping(
                config.db_path,
                provider="crunchyroll",
                provider_series_id="series-123",
                mal_anime_id=321,
                confidence=0.99,
                mapping_source="user_approved",
                approved_by_user=True,
                notes="looked correct",
            )
            items = list_series_mappings(config.db_path, provider="crunchyroll", approved_only=True)

        self.assertEqual(created.mal_anime_id, 321)
        self.assertEqual(len(items), 1)
        self.assertTrue(items[0].approved_by_user)
        self.assertEqual(items[0].notes, "looked correct")


class DryRunPlannerTests(unittest.TestCase):
    def test_build_dry_run_sync_plan_proposes_forward_only_update(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["series"][0]["title"] = "Example Show"
            payload["series"][0]["season_title"] = "Example Show (English Dub)"
            payload["progress"][0]["episode_number"] = 3
            payload["progress"][0]["completion_ratio"] = 0.95
            ingest_snapshot_payload(payload, config)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
            (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 123,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": ["Example Show (English Dub)"]},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 123,
                    "title": "Example Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "watching", "num_episodes_watched": 1},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)

        self.assertEqual(len(proposals), 1)
        proposal = proposals[0]
        self.assertEqual(proposal.decision, "propose_update")
        self.assertEqual(proposal.proposed_my_list_status, {"status": "watching", "num_watched_episodes": 3})
        self.assertEqual(proposal.mapping_source, "auto_exact")
        self.assertTrue(proposal.persisted_mapping_approved)
        self.assertIn("preserve_meaningful_score", proposal.reasons)

    def test_build_dry_run_sync_plan_refuses_to_decrease_existing_progress(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"][0]["completion_ratio"] = 0.95
            ingest_snapshot_payload(payload, config)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
            (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 123,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 123,
                    "title": "Example Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "watching", "num_episodes_watched": 5},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)

        self.assertEqual(proposals[0].decision, "skip")
        self.assertTrue(any("refusing_to_decrease_mal_progress" in reason for reason in proposals[0].reasons))

    def test_build_mapping_review_preserves_user_approved_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            ingest_snapshot_payload(payload, config)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
            (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")
            upsert_series_mapping(
                config.db_path,
                provider="crunchyroll",
                provider_series_id="series-123",
                mal_anime_id=777,
                confidence=1.0,
                mapping_source="user_approved",
                approved_by_user=True,
                notes="manual approval",
            )

            with patch.object(MalClient, "search_anime", side_effect=AssertionError("should not search approved mapping")):
                items = build_mapping_review(config, limit=5, mapping_limit=3)

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].decision, "preserved")
        self.assertEqual(items[0].suggested_mal_anime_id, 777)
        self.assertEqual(items[0].mapping_status, "approved")

    def test_build_mapping_review_auto_approves_exact_unique_match(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["series"][0]["title"] = "Example Show"
            payload["series"][0]["season_title"] = "Example Show Season 2 (English Dub)"
            payload["series"][0]["season_number"] = 2
            payload["progress"][0]["episode_number"] = 12
            payload["progress"][0]["completion_ratio"] = 0.95
            ingest_snapshot_payload(payload, config)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
            (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 222,
                                "title": "Example Show Season 2",
                                "alternative_titles": {"synonyms": ["Example Show Season 2 (English Dub)"]},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {"node": {"id": 999, "title": "Different Show", "alternative_titles": {}, "media_type": "tv"}},
                    ]
                },
            ):
                items = build_mapping_review(config, limit=5, mapping_limit=3)
                persisted = list_series_mappings(config.db_path, provider="crunchyroll", approved_only=True)

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].decision, "auto_approved")
        self.assertEqual(items[0].mapping_status, "approved")
        self.assertTrue(any(reason == "auto_approved_exact_unique_match" for reason in items[0].reasons))
        self.assertEqual(len(persisted), 1)
        self.assertEqual(persisted[0].mal_anime_id, 222)
        self.assertEqual(persisted[0].mapping_source, "auto_exact")

    def test_build_mapping_review_keeps_level_two_title_as_auto_approved_one_to_one(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["series"][0]["title"] = "Chillin’ in Another World with Level 2 Super Cheat Powers"
            payload["series"][0]["season_title"] = "Chillin’ in Another World with Level 2 Super Cheat Powers"
            payload["series"][0]["season_number"] = 1
            _replace_progress_with_completed_episodes(payload, "series-123", 12)
            ingest_snapshot_payload(payload, config)
            _write_test_mal_secret_files(root)

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 56923,
                                "title": "Lv2 kara Cheat datta Motoyuusha Kouho no Mattari Isekai Life",
                                "alternative_titles": {"en": "Chillin’ in Another World with Level 2 Super Cheat Powers", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 64553,
                                "title": "Lv2 kara Cheat datta Motoyuusha Kouho no Mattari Isekai Life 2nd Season",
                                "alternative_titles": {"en": "Chillin’ in Another World with Level 2 Super Cheat Powers Season 2", "synonyms": []},
                                "media_type": "tv",
                                "status": "not_yet_aired",
                                "num_episodes": 0,
                            }
                        },
                    ]
                },
            ):
                items = build_mapping_review(config, limit=5, mapping_limit=3)
                persisted = list_series_mappings(config.db_path, provider="crunchyroll", approved_only=True)

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].decision, "auto_approved")
        self.assertEqual(items[0].mapping_status, "approved")
        self.assertEqual(items[0].suggested_mal_anime_id, 56923)
        self.assertEqual([56923], [item.mal_anime_id for item in persisted])

    def test_build_mapping_review_auto_classifies_deterministic_split_bundle_without_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["series"][0]["provider_series_id"] = "GY243NN0R"
            payload["series"][0]["title"] = "The Misfit of Demon King Academy"
            payload["series"][0]["season_title"] = "The Misfit of Demon King Academy II(English Dub)"
            payload["series"][0]["season_number"] = 2
            payload["watchlist"][0]["provider_series_id"] = "GY243NN0R"
            _replace_progress_with_completed_episodes(payload, "GY243NN0R", 24)
            ingest_snapshot_payload(payload, config)
            _write_test_mal_secret_files(root)

            with patch.object(MalClient, "search_anime", return_value=_misfit_split_part_search_response()):
                items = build_mapping_review(config, limit=5, mapping_limit=5)
                persisted = list_series_mappings(config.db_path, provider="crunchyroll")
                queue_result = persist_mapping_review_queue(config, items)
                open_rows = list_review_queue_entries(config.db_path, status="open", issue_type="mapping_review")

        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item.decision, "auto_classified_bundle")
        self.assertIsNone(item.existing_mapping)
        self.assertEqual(item.suggested_mal_anime_id, 48417)
        self.assertEqual({48418}, {candidate["mal_anime_id"] for candidate in item.bundle_companion_candidates})
        self.assertIn("multi_entry_bundle_suspected=24<=12+12", item.reasons)
        self.assertIn("auto_classified_multi_entry_bundle_non_actionable", item.reasons)
        self.assertEqual([], persisted)
        self.assertEqual({"resolved": 0, "inserted": 0}, queue_result)
        self.assertEqual([], open_rows)

    def test_build_mapping_review_auto_classifies_verified_gintama_aggregate_shell_without_mapping_or_queue(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            bootstrap_database(config.db_path)
            _write_test_mal_secret_files(root)
            with connect(config.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO provider_series(provider, provider_series_id, title, season_title, season_number, raw_json, account_observed_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (
                        "crunchyroll",
                        "GYQ4MKDZ6",
                        "Gintama",
                        "Gintama",
                        None,
                        json.dumps({"raw": {"series_metadata": {"episode_count": 382, "season_count": 8}}}),
                    ),
                )
                conn.commit()
            upsert_recommendation_provider_eligibility_evidence(
                config.db_path,
                mal_anime_id=918,
                provider="crunchyroll",
                provider_series_id="GYQ4MKDZ6",
                provider_title="Gintama",
                identity_match_kind="provider_title_search_exact",
                match_confidence=0.9,
                review_status="verified",
                catalog_status="present",
                english_dub_status="present",
                fetched_at="2026-07-24T00:00:00Z",
                expires_at="2026-07-31T00:00:00Z",
                source_evidence={"identity_evidence": None},
            )
            gintama_918 = {
                "id": 918,
                "title": "Gintama",
                "alternative_titles": {"en": "Gintama", "synonyms": []},
                "media_type": "tv",
                "status": "finished_airing",
                "num_episodes": 201,
            }

            with patch.object(MalClient, "search_anime", return_value={"data": [{"node": gintama_918}]}), patch.object(
                MalClient,
                "get_anime_details",
                return_value=gintama_918,
            ):
                items = build_mapping_review(config, limit=5, mapping_limit=5, provider_series_ids=["GYQ4MKDZ6"])
                persisted = list_series_mappings(config.db_path, provider="crunchyroll")
                queue_result = persist_mapping_review_queue(config, items)
                open_rows = list_review_queue_entries(config.db_path, status="open", issue_type="mapping_review")

        self.assertEqual(1, len(items))
        item = items[0]
        self.assertEqual("auto_classified_provider_shell", item.decision)
        self.assertEqual("ambiguous", item.mapping_status)
        self.assertIsNone(item.existing_mapping)
        self.assertEqual(918, item.suggested_mal_anime_id)
        self.assertIn("auto_classified_provider_shell_non_actionable", item.reasons)
        self.assertIn("provider_aggregate_shell_non_actionable", item.reasons)
        self.assertTrue(any(reason.startswith("provider_aggregate_shell_suspected=provider_episodes:382;candidate_episodes:201;provider_seasons:8") for reason in item.reasons))
        self.assertEqual([], persisted)
        self.assertEqual({"resolved": 0, "inserted": 0}, queue_result)
        self.assertEqual([], open_rows)

    def test_build_mapping_review_auto_classifies_verified_rascal_multi_child_shell_without_child_mapping_or_queue(self) -> None:
        identity_evidence = {
            "child_titles": [
                {"title": "Rascal Does Not Dream of Bunny Girl Senpai", "episode_count": 13},
                {"title": "Rascal Does Not Dream of a Dreaming Girl", "episode_count": 1},
                {"title": "Rascal Does Not Dream of a Sister Venturing Out", "episode_count": 1},
                {"title": "Rascal Does Not Dream of a Knapsack Kid", "episode_count": 1},
                {"title": "Rascal Does Not Dream of Santa Claus", "episode_count": 13},
            ],
            "parent_episode_count": 29,
            "parent_season_count": 5,
        }
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            bootstrap_database(config.db_path)
            _write_test_mal_secret_files(root)
            with connect(config.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO provider_series(provider, provider_series_id, title, season_title, season_number, raw_json, account_observed_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (
                        "crunchyroll",
                        "GYW4MG9G6",
                        "Rascal Does Not Dream Series",
                        "Rascal Does Not Dream Series",
                        None,
                        json.dumps({"identity_evidence": identity_evidence, "raw": {"series_metadata": {"episode_count": 29, "season_count": 5}}}),
                    ),
                )
                conn.commit()
            upsert_recommendation_provider_eligibility_evidence(
                config.db_path,
                mal_anime_id=37450,
                provider="crunchyroll",
                provider_series_id="GYW4MG9G6",
                provider_title="Rascal Does Not Dream Series",
                identity_match_kind="provider_franchise_shell_child_match",
                match_confidence=0.88,
                review_status="verified",
                catalog_status="present",
                english_dub_status="present",
                fetched_at="2026-07-24T00:00:00Z",
                expires_at="2026-07-31T00:00:00Z",
                source_evidence={"identity_evidence": identity_evidence},
            )
            child_nodes = [
                {
                    "id": 37450,
                    "title": "Seishun Buta Yarou wa Bunny Girl Senpai no Yume wo Minai",
                    "alternative_titles": {"en": "Rascal Does Not Dream of Bunny Girl Senpai", "synonyms": []},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 13,
                    "related_anime": [
                        {"node": {"id": 38329}},
                        {"node": {"id": 53129}},
                    ],
                },
                {
                    "id": 38329,
                    "title": "Seishun Buta Yarou wa Yumemiru Shoujo no Yume wo Minai",
                    "alternative_titles": {"en": "Rascal Does Not Dream of a Dreaming Girl", "synonyms": []},
                    "media_type": "movie",
                    "status": "finished_airing",
                    "num_episodes": 1,
                    "related_anime": [],
                },
                {
                    "id": 53129,
                    "title": "Seishun Buta Yarou wa Odekake Sister no Yume wo Minai",
                    "alternative_titles": {"en": "Rascal Does Not Dream of a Sister Venturing Out", "synonyms": []},
                    "media_type": "movie",
                    "status": "finished_airing",
                    "num_episodes": 1,
                    "related_anime": [],
                },
            ]
            detail_by_id = {node["id"]: node for node in child_nodes}

            with patch.object(MalClient, "search_anime", return_value={"data": [{"node": child_nodes[0]}]}), patch.object(
                MalClient,
                "get_anime_details",
                side_effect=lambda anime_id, fields=None: detail_by_id[anime_id],
            ):
                items = build_mapping_review(config, limit=5, mapping_limit=5, provider_series_ids=["GYW4MG9G6"])
                persisted = list_series_mappings(config.db_path, provider="crunchyroll")
                queue_result = persist_mapping_review_queue(config, items)
                open_rows = list_review_queue_entries(config.db_path, status="open", issue_type="mapping_review")

        self.assertEqual(1, len(items))
        item = items[0]
        self.assertEqual("auto_classified_provider_shell", item.decision)
        self.assertEqual("ambiguous", item.mapping_status)
        self.assertIsNone(item.existing_mapping)
        self.assertEqual(37450, item.suggested_mal_anime_id)
        self.assertNotIn(38329, {candidate["mal_anime_id"] for candidate in item.candidates})
        self.assertNotIn(53129, {candidate["mal_anime_id"] for candidate in item.candidates})
        self.assertIn("auto_classified_provider_shell_non_actionable", item.reasons)
        self.assertIn("provider_aggregate_shell_non_actionable", item.reasons)
        self.assertTrue(any(reason.startswith("provider_franchise_shell_child_match_non_actionable=children:5") for reason in item.reasons))
        self.assertEqual([], persisted)
        self.assertEqual({"resolved": 0, "inserted": 0}, queue_result)
        self.assertEqual([], open_rows)

    def test_build_mapping_review_keeps_ambiguous_aggregate_shell_in_review_queue(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            bootstrap_database(config.db_path)
            _write_test_mal_secret_files(root)
            with connect(config.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO provider_series(provider, provider_series_id, title, season_title, season_number, raw_json, account_observed_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (
                        "crunchyroll",
                        "ambiguous-aggregate",
                        "Example Saga",
                        "Example Saga",
                        None,
                        json.dumps({"raw": {"series_metadata": {"episode_count": 36, "season_count": 3}}}),
                    ),
                )
                conn.commit()
            candidate = {
                "id": 70001,
                "title": "Example Saga",
                "alternative_titles": {"en": "Example Saga", "synonyms": []},
                "media_type": "tv",
                "status": "finished_airing",
                "num_episodes": 12,
            }

            with patch.object(MalClient, "search_anime", return_value={"data": [{"node": candidate}]}):
                items = build_mapping_review(config, limit=5, mapping_limit=5, provider_series_ids=["ambiguous-aggregate"])
                persisted = list_series_mappings(config.db_path, provider="crunchyroll")
                queue_result = persist_mapping_review_queue(config, items)
                open_rows = list_review_queue_entries(config.db_path, status="open", issue_type="mapping_review")

        self.assertEqual(1, len(items))
        item = items[0]
        self.assertEqual("needs_review", item.decision)
        self.assertEqual("ambiguous", item.mapping_status)
        self.assertIn("provider_aggregate_shell_non_actionable", item.reasons)
        self.assertNotIn("auto_classified_provider_shell_non_actionable", item.reasons)
        self.assertTrue(any(reason.startswith("provider_aggregate_shell_suspected=provider_episodes:36;candidate_episodes:12;provider_seasons:3") for reason in item.reasons))
        self.assertEqual([], persisted)
        self.assertEqual({"resolved": 0, "inserted": 1}, queue_result)
        self.assertEqual(["ambiguous-aggregate"], [row.provider_series_id for row in open_rows])

    def test_build_mapping_review_classifies_dororo_exact_title_tie_without_queue_or_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            bootstrap_database(config.db_path)
            _write_test_mal_secret_files(root)
            with connect(config.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO provider_series(provider, provider_series_id, title, season_title, season_number, raw_json, account_observed_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    ("hidive", "1181", "Dororo", "Dororo", None, json.dumps({"title": "Dororo", "season_title": "Dororo"})),
                )
                conn.commit()

            dororo_nodes = [
                {
                    "id": 5760,
                    "title": "Dororo to Hyakkimaru",
                    "alternative_titles": {"en": "Dororo", "synonyms": ["Dororo"]},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 26,
                    "start_season": {"season": "spring", "year": 1969},
                },
                {
                    "id": 37520,
                    "title": "Dororo",
                    "alternative_titles": {"en": "Dororo", "synonyms": ["Dororo to Hyakkimaru"]},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 24,
                    "start_season": {"season": "winter", "year": 2019},
                },
            ]

            with patch.object(MalClient, "search_anime", return_value={"data": [{"node": node} for node in dororo_nodes]}):
                items = build_mapping_review(config, limit=5, mapping_limit=5, provider_series_ids=["1181"])
                persisted = list_series_mappings(config.db_path, provider="hidive")
                queue_result = persist_mapping_review_queue(config, items)
                open_rows = list_review_queue_entries(config.db_path, status="open", issue_type="mapping_review")

        self.assertEqual(1, len(items))
        self.assertEqual("auto_classified_ambiguous_exact_title", items[0].decision)
        self.assertIn("auto_classified_ambiguous_exact_title_non_actionable", items[0].reasons)
        self.assertEqual([], persisted)
        self.assertEqual({"resolved": 0, "inserted": 0}, queue_result)
        self.assertEqual([], open_rows)

    def test_build_mapping_review_classifies_provider_movie_collection_shell_without_queue_or_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["series"][0]["provider_series_id"] = "code-geass-movies"
            payload["series"][0]["title"] = "Code Geass"
            payload["series"][0]["season_title"] = "Code Geass: Lelouch of the Rebellion Movies"
            payload["series"][0]["season_number"] = 13
            payload["watchlist"][0]["provider_series_id"] = "code-geass-movies"
            _replace_progress_with_completed_episodes(payload, "code-geass-movies", 25)
            ingest_snapshot_payload(payload, config)
            _write_test_mal_secret_files(root)
            candidates = [
                {
                    "id": 1575,
                    "title": "Code Geass: Hangyaku no Lelouch",
                    "alternative_titles": {"en": "Code Geass: Lelouch of the Rebellion", "synonyms": []},
                    "media_type": "tv",
                    "num_episodes": 25,
                },
                {
                    "id": 34438,
                    "title": "Code Geass: Hangyaku no Lelouch I - Koudou",
                    "alternative_titles": {"en": "Code Geass: Lelouch of the Rebellion I - Initiation", "synonyms": []},
                    "media_type": "movie",
                    "num_episodes": 1,
                },
            ]

            with patch.object(MalClient, "search_anime", return_value={"data": [{"node": node} for node in candidates]}):
                items = build_mapping_review(config, limit=5, mapping_limit=5)
                persisted = list_series_mappings(config.db_path, provider="crunchyroll")
                queue_result = persist_mapping_review_queue(config, items)
                open_rows = list_review_queue_entries(config.db_path, status="open", issue_type="mapping_review")

        self.assertEqual(1, len(items))
        self.assertEqual("auto_classified_provider_shell", items[0].decision)
        self.assertIn("auto_classified_provider_shell_non_actionable", items[0].reasons)
        self.assertTrue(any(reason.startswith("provider_aggregate_title_shell_non_actionable=") for reason in items[0].reasons))
        self.assertEqual([], persisted)
        self.assertEqual({"resolved": 0, "inserted": 0}, queue_result)
        self.assertEqual([], open_rows)

    def test_build_mapping_review_keeps_uncertain_alias_only_bundle_in_review_queue(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["series"][0]["title"] = "The Melancholy of Haruhi Suzumiya"
            payload["series"][0]["season_title"] = "The Melancholy of Haruhi Suzumiya (English Dub)"
            payload["series"][0]["season_number"] = 1
            _replace_progress_with_completed_episodes(payload, "series-123", 28)
            ingest_snapshot_payload(payload, config)
            _write_test_mal_secret_files(root)

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 849,
                                "title": "Suzumiya Haruhi no Yuuutsu",
                                "alternative_titles": {"en": "The Melancholy of Haruhi Suzumiya", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 14,
                            }
                        },
                        {
                            "node": {
                                "id": 4382,
                                "title": "Suzumiya Haruhi no Yuuutsu (2009)",
                                "alternative_titles": {"en": "The Melancholy of Haruhi Suzumiya", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 14,
                            }
                        },
                        {
                            "node": {
                                "id": 9000,
                                "title": "The Melancholy of Haruhi",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 28,
                            }
                        },
                    ]
                },
            ):
                items = build_mapping_review(config, limit=5, mapping_limit=5)
                queue_result = persist_mapping_review_queue(config, items)
                open_rows = list_review_queue_entries(config.db_path, status="open", issue_type="mapping_review")

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].decision, "auto_classified_ambiguous_exact_title")
        self.assertIn("multi_entry_bundle_suspected=28<=14+14", items[0].reasons)
        self.assertIn("auto_classified_ambiguous_exact_title_non_actionable", items[0].reasons)
        self.assertEqual({"resolved": 0, "inserted": 0}, queue_result)
        self.assertEqual([], [row.provider_series_id for row in open_rows])

    def test_build_mapping_review_surfaces_bundle_companion_for_multi_entry_residue(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["series"][0]["title"] = "The Melancholy of Haruhi Suzumiya"
            payload["series"][0]["season_title"] = "The Melancholy of Haruhi Suzumiya (English Dub)"
            payload["series"][0]["season_number"] = 1
            progress_template = dict(payload["progress"][0])
            payload["progress"] = []
            for episode_number in range(1, 29):
                item = dict(progress_template)
                item["provider_series_id"] = "series-123"
                item["provider_episode_id"] = f"ep-{episode_number}"
                item["episode_number"] = episode_number
                item["completion_ratio"] = 1.0
                payload["progress"].append(item)
            ingest_snapshot_payload(payload, config)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
            (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 849,
                                "title": "Suzumiya Haruhi no Yuuutsu",
                                "alternative_titles": {"en": "The Melancholy of Haruhi Suzumiya", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 14,
                            }
                        },
                        {
                            "node": {
                                "id": 4382,
                                "title": "Suzumiya Haruhi no Yuuutsu (2009)",
                                "alternative_titles": {"en": "The Melancholy of Haruhi Suzumiya", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 14,
                            }
                        },
                    ]
                },
            ):
                items = build_mapping_review(config, limit=5, mapping_limit=5)

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].decision, "auto_classified_ambiguous_exact_title")
        self.assertEqual(MAPPING_REVIEW_HEURISTICS_REVISION, items[0].mapper_revision)
        self.assertEqual(MAPPING_REVIEW_HEURISTICS_REVISION, items[0].as_dict()["mapper_revision"])
        self.assertEqual(849, items[0].suggested_mal_anime_id)
        self.assertIsNotNone(items[0].bundle_companion_candidate)
        self.assertEqual(4382, items[0].bundle_companion_candidate["mal_anime_id"])
        self.assertEqual(items[0].bundle_companion_candidate["num_episodes"], 14)
        self.assertEqual(1, len(items[0].bundle_companion_candidates))
        self.assertEqual({4382}, {candidate["mal_anime_id"] for candidate in items[0].bundle_companion_candidates})
        self.assertIn("auto_classified_ambiguous_exact_title_non_actionable", items[0].reasons)

    def test_build_mapping_review_surfaces_all_bundle_companions_for_three_entry_residue(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["series"][0]["provider_series_id"] = "series-123b"
            payload["series"][0]["title"] = "Example Split Show"
            payload["series"][0]["season_title"] = "Example Split Show (English Dub)"
            payload["series"][0]["season_number"] = 1
            payload["watchlist"][0]["provider_series_id"] = "series-123b"
            progress_template = dict(payload["progress"][0])
            payload["progress"] = []
            for episode_number in range(1, 37):
                item = dict(progress_template)
                item["provider_series_id"] = "series-123b"
                item["provider_episode_id"] = f"ep-{episode_number}"
                item["episode_number"] = episode_number
                item["completion_ratio"] = 1.0
                payload["progress"].append(item)
            ingest_snapshot_payload(payload, config)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
            (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 1001,
                                "title": "Example Split Show",
                                "alternative_titles": {"en": "Example Split Show", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 1002,
                                "title": "Example Split Show Part 2",
                                "alternative_titles": {"en": "Example Split Show", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 1003,
                                "title": "Example Split Show Part 3",
                                "alternative_titles": {"en": "Example Split Show", "synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                    ]
                },
            ):
                items = build_mapping_review(config, limit=5, mapping_limit=5)

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].decision, "auto_classified_aggregate_progress")
        self.assertEqual({1002, 1003}, {candidate["mal_anime_id"] for candidate in items[0].bundle_companion_candidates})
        self.assertIn(items[0].bundle_companion_candidate["mal_anime_id"], {1002, 1003})
        self.assertIn("auto_classified_aggregate_progress_non_actionable", items[0].reasons)

    def test_build_dry_run_sync_plan_uses_user_approved_mapping_without_search(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"][0]["episode_number"] = 2
            payload["progress"][0]["completion_ratio"] = 0.95
            ingest_snapshot_payload(payload, config)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
            (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")
            upsert_series_mapping(
                config.db_path,
                provider="crunchyroll",
                provider_series_id="series-123",
                mal_anime_id=555,
                confidence=1.0,
                mapping_source="user_approved",
                approved_by_user=True,
                notes=None,
            )

            with patch.object(MalClient, "search_anime", side_effect=AssertionError("should not search approved mapping")), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 555,
                    "title": "Approved Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "watching", "num_episodes_watched": 0},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)

        self.assertEqual(len(proposals), 1)
        self.assertEqual(proposals[0].mal_anime_id, 555)
        self.assertTrue(proposals[0].persisted_mapping_approved)
        self.assertEqual(proposals[0].mapping_status, "approved")
        self.assertEqual(proposals[0].decision, "propose_update")

    def test_build_dry_run_sync_plan_auto_approves_exact_unique_match_for_sync(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["series"][0]["title"] = "Example Show"
            payload["series"][0]["season_title"] = "Example Show Season 2 (English Dub)"
            payload["series"][0]["season_number"] = 2
            payload["progress"][0]["episode_number"] = 12
            payload["progress"][0]["completion_ratio"] = 0.95
            ingest_snapshot_payload(payload, config)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
            (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 333,
                                "title": "Example Show Season 2",
                                "alternative_titles": {"synonyms": ["Example Show Season 2 (English Dub)"]},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {"node": {"id": 999, "title": "Different Show", "alternative_titles": {}, "media_type": "tv"}},
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 333,
                    "title": "Example Show Season 2",
                    "num_episodes": 12,
                    "my_list_status": {"status": "watching", "num_episodes_watched": 0},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)
                persisted = list_series_mappings(config.db_path, provider="crunchyroll", approved_only=True)

        self.assertEqual(len(proposals), 1)
        self.assertEqual(proposals[0].mapping_status, "approved")
        self.assertTrue(proposals[0].persisted_mapping_approved)
        self.assertEqual(proposals[0].mapping_source, "auto_exact")
        self.assertTrue(any(reason == "auto_approved_exact_unique_match" for reason in proposals[0].reasons))
        self.assertEqual(len(persisted), 1)
        self.assertEqual(persisted[0].mal_anime_id, 333)

    def test_build_dry_run_sync_plan_keeps_deterministic_split_bundle_unmapped(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["series"][0]["provider_series_id"] = "GY243NN0R"
            payload["series"][0]["title"] = "The Misfit of Demon King Academy"
            payload["series"][0]["season_title"] = "The Misfit of Demon King Academy II(English Dub)"
            payload["series"][0]["season_number"] = 2
            payload["watchlist"][0]["provider_series_id"] = "GY243NN0R"
            _replace_progress_with_completed_episodes(payload, "GY243NN0R", 24)
            ingest_snapshot_payload(payload, config)
            _write_test_mal_secret_files(root)

            search_response = _misfit_split_part_search_response()
            detail_by_id = {entry["node"]["id"]: {**entry["node"], "related_anime": []} for entry in search_response["data"]}

            def fake_get_anime_details(anime_id: int, fields: str | None = None) -> dict:
                if fields and "my_list_status" in fields:
                    raise AssertionError("deterministic bundle must not resolve to one MAL status details lookup")
                return detail_by_id[anime_id]

            with patch.object(MalClient, "search_anime", return_value=search_response), patch.object(
                MalClient,
                "get_anime_details",
                side_effect=fake_get_anime_details,
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=5)
                persisted = list_series_mappings(config.db_path, provider="crunchyroll")

        self.assertEqual(len(proposals), 1)
        proposal = proposals[0]
        self.assertEqual(proposal.decision, "review")
        self.assertIsNone(proposal.mal_anime_id)
        self.assertFalse(proposal.persisted_mapping_approved)
        self.assertIn("auto_classified_multi_entry_bundle_non_actionable", proposal.reasons)
        self.assertEqual([], persisted)

    def test_build_dry_run_sync_plan_keeps_aggregate_progress_exact_installment_unmapped(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["series"][0]["provider_series_id"] = "blue-night-saga"
            payload["series"][0]["title"] = "Blue Exorcist"
            payload["series"][0]["season_title"] = "Blue Exorcist -The Blue Night Saga-"
            payload["series"][0]["season_number"] = 5
            payload["watchlist"][0]["provider_series_id"] = "blue-night-saga"
            _replace_progress_with_completed_episodes(payload, "blue-night-saga", 25)
            ingest_snapshot_payload(payload, config)
            _write_test_mal_secret_files(root)
            candidate = {
                "id": 59226,
                "title": "Ao no Exorcist: Yosuga-hen",
                "alternative_titles": {"en": "Blue Exorcist: The Blue Night Saga", "synonyms": ["Blue Exorcist Season 5"]},
                "media_type": "tv",
                "num_episodes": 12,
            }

            def fake_get_anime_details(anime_id: int, fields: str | None = None) -> dict:
                if fields and "my_list_status" in fields:
                    raise AssertionError("aggregate progress must not resolve to one MAL status details lookup")
                raise MalApiError("offline")

            with patch.object(MalClient, "search_anime", return_value={"data": [{"node": candidate}]}), patch.object(
                MalClient,
                "get_anime_details",
                side_effect=fake_get_anime_details,
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=5)
                persisted = list_series_mappings(config.db_path, provider="crunchyroll")

        self.assertEqual(len(proposals), 1)
        proposal = proposals[0]
        self.assertEqual(proposal.decision, "review")
        self.assertIsNone(proposal.mal_anime_id)
        self.assertFalse(proposal.persisted_mapping_approved)
        self.assertIn("auto_classified_aggregate_progress_non_actionable", proposal.reasons)
        self.assertTrue(any(reason.startswith("aggregate_progress_non_actionable=") for reason in proposal.reasons))
        self.assertEqual([], persisted)

    def test_build_dry_run_sync_plan_keeps_verified_gintama_aggregate_unmapped(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            bootstrap_database(config.db_path)
            _write_test_mal_secret_files(root)
            with connect(config.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO provider_series(provider, provider_series_id, title, season_title, season_number, raw_json, account_observed_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (
                        "crunchyroll",
                        "GYQ4MKDZ6",
                        "Gintama",
                        "Gintama",
                        None,
                        json.dumps({"raw": {"series_metadata": {"episode_count": 382, "season_count": 8}}}),
                    ),
                )
                conn.commit()
            upsert_recommendation_provider_eligibility_evidence(
                config.db_path,
                mal_anime_id=918,
                provider="crunchyroll",
                provider_series_id="GYQ4MKDZ6",
                provider_title="Gintama",
                identity_match_kind="provider_title_search_exact",
                match_confidence=0.9,
                review_status="verified",
                catalog_status="present",
                english_dub_status="present",
                fetched_at="2026-07-24T00:00:00Z",
                expires_at="2026-07-31T00:00:00Z",
                source_evidence={"identity_evidence": None},
            )
            gintama_918 = {
                "id": 918,
                "title": "Gintama",
                "alternative_titles": {"en": "Gintama", "synonyms": []},
                "media_type": "tv",
                "status": "finished_airing",
                "num_episodes": 201,
            }

            def fake_get_anime_details(anime_id: int, fields: str | None = None) -> dict:
                if fields and "my_list_status" in fields:
                    raise AssertionError("aggregate shell must not resolve to one MAL status details lookup")
                self.assertEqual(918, anime_id)
                return gintama_918

            with patch.object(MalClient, "search_anime", return_value={"data": [{"node": gintama_918}]}), patch.object(
                MalClient,
                "get_anime_details",
                side_effect=fake_get_anime_details,
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=5)
                persisted = list_series_mappings(config.db_path, provider="crunchyroll")

        self.assertEqual(len(proposals), 1)
        proposal = proposals[0]
        self.assertEqual("review", proposal.decision)
        self.assertIsNone(proposal.mal_anime_id)
        self.assertFalse(proposal.persisted_mapping_approved)
        self.assertIn("provider_aggregate_shell_non_actionable", proposal.reasons)
        self.assertTrue(any(reason.startswith("provider_aggregate_shell_suspected=") for reason in proposal.reasons))
        self.assertEqual([], persisted)

    def test_build_dry_run_sync_plan_keeps_verified_rascal_multi_child_shell_unmapped(self) -> None:
        identity_evidence = {
            "child_titles": [
                {"title": "Rascal Does Not Dream of Bunny Girl Senpai", "episode_count": 13},
                {"title": "Rascal Does Not Dream of a Dreaming Girl", "episode_count": 1},
                {"title": "Rascal Does Not Dream of a Sister Venturing Out", "episode_count": 1},
                {"title": "Rascal Does Not Dream of a Knapsack Kid", "episode_count": 1},
                {"title": "Rascal Does Not Dream of Santa Claus", "episode_count": 13},
            ],
            "parent_episode_count": 29,
            "parent_season_count": 5,
        }
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            bootstrap_database(config.db_path)
            _write_test_mal_secret_files(root)
            with connect(config.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO provider_series(provider, provider_series_id, title, season_title, season_number, raw_json, account_observed_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (
                        "crunchyroll",
                        "GYW4MG9G6",
                        "Rascal Does Not Dream Series",
                        "Rascal Does Not Dream Series",
                        None,
                        json.dumps(
                            {
                                "identity_evidence": identity_evidence,
                                "raw": {"series_metadata": {"episode_count": 29, "season_count": 5}},
                            }
                        ),
                    ),
                )
                conn.commit()
            upsert_recommendation_provider_eligibility_evidence(
                config.db_path,
                mal_anime_id=37450,
                provider="crunchyroll",
                provider_series_id="GYW4MG9G6",
                provider_title="Rascal Does Not Dream Series",
                identity_match_kind="provider_franchise_shell_child_match",
                match_confidence=0.88,
                review_status="verified",
                catalog_status="present",
                english_dub_status="present",
                fetched_at="2026-07-24T00:00:00Z",
                expires_at="2026-07-31T00:00:00Z",
                source_evidence={"identity_evidence": identity_evidence},
            )
            bunny_girl = {
                "id": 37450,
                "title": "Seishun Buta Yarou wa Bunny Girl Senpai no Yume wo Minai",
                "alternative_titles": {"en": "Rascal Does Not Dream of Bunny Girl Senpai", "synonyms": []},
                "media_type": "tv",
                "status": "finished_airing",
                "num_episodes": 13,
            }

            def fake_get_anime_details(anime_id: int, fields: str | None = None) -> dict:
                if fields and "my_list_status" in fields:
                    raise AssertionError("multi-child franchise shell must not resolve to one MAL status details lookup")
                self.assertEqual(37450, anime_id)
                return bunny_girl

            with patch.object(MalClient, "search_anime", return_value={"data": [{"node": bunny_girl}]}), patch.object(
                MalClient,
                "get_anime_details",
                side_effect=fake_get_anime_details,
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=5)
                persisted = list_series_mappings(config.db_path, provider="crunchyroll")

        self.assertEqual(len(proposals), 1)
        proposal = proposals[0]
        self.assertEqual("review", proposal.decision)
        self.assertIsNone(proposal.mal_anime_id)
        self.assertFalse(proposal.persisted_mapping_approved)
        self.assertIn("provider_aggregate_shell_non_actionable", proposal.reasons)
        self.assertTrue(any(reason.startswith("provider_franchise_shell_child_match_non_actionable=") for reason in proposal.reasons))
        self.assertEqual([], persisted)

    def test_build_dry_run_sync_plan_classified_provider_shell_creates_no_mapping_or_write_plan(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            bootstrap_database(config.db_path)
            _write_test_mal_secret_files(root)
            with connect(config.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO provider_series(provider, provider_series_id, title, season_title, season_number, raw_json, account_observed_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (
                        "crunchyroll",
                        "GYQ4MKDZ6",
                        "Gintama",
                        "Gintama",
                        None,
                        json.dumps({"raw": {"series_metadata": {"episode_count": 382, "season_count": 8}}}),
                    ),
                )
                conn.commit()
            upsert_recommendation_provider_eligibility_evidence(
                config.db_path,
                mal_anime_id=918,
                provider="crunchyroll",
                provider_series_id="GYQ4MKDZ6",
                provider_title="Gintama",
                identity_match_kind="provider_title_search_exact",
                match_confidence=0.9,
                review_status="verified",
                catalog_status="present",
                english_dub_status="present",
                fetched_at="2026-07-24T00:00:00Z",
                expires_at="2026-07-31T00:00:00Z",
                source_evidence={"identity_evidence": None},
            )
            gintama_918 = {
                "id": 918,
                "title": "Gintama",
                "alternative_titles": {"en": "Gintama", "synonyms": []},
                "media_type": "tv",
                "status": "finished_airing",
                "num_episodes": 201,
            }

            def fake_get_anime_details(anime_id: int, fields: str | None = None) -> dict:
                if fields and "my_list_status" in fields:
                    raise AssertionError("classified provider shell must not resolve to one MAL write details lookup")
                return gintama_918

            with patch.object(MalClient, "search_anime", return_value={"data": [{"node": gintama_918}]}), patch.object(
                MalClient,
                "get_anime_details",
                side_effect=fake_get_anime_details,
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=5)
                persisted = list_series_mappings(config.db_path, provider="crunchyroll")

        self.assertEqual(1, len(proposals))
        proposal = proposals[0]
        self.assertEqual("review", proposal.decision)
        self.assertEqual("ambiguous", proposal.mapping_status)
        self.assertIsNone(proposal.mal_anime_id)
        self.assertFalse(proposal.persisted_mapping_approved)
        self.assertIn("auto_classified_provider_shell_non_actionable", proposal.reasons)
        self.assertIn("provider_aggregate_shell_non_actionable", proposal.reasons)
        self.assertEqual([], persisted)

    def test_build_dry_run_sync_plan_can_require_approved_mappings_only(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            ingest_snapshot_payload(payload, config)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
            (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(MalClient, "search_anime", side_effect=AssertionError("approved-only should not live search")):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3, approved_mappings_only=True)

        self.assertEqual(len(proposals), 1)
        self.assertEqual(proposals[0].decision, "review")
        self.assertTrue(any(reason == "approved_mappings_only_enabled" for reason in proposals[0].reasons))

    def test_persist_mapping_review_queue_only_keeps_unresolved_items(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            ingest_snapshot_payload(payload, config)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
            (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(MalClient, "search_anime", return_value={"data": []}):
                items = build_mapping_review(config, limit=5, mapping_limit=3)
            persist_mapping_review_queue(config, items)
            rows = list_review_queue_entries(config.db_path, status="open", issue_type="mapping_review")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].severity, "error")
        self.assertEqual(rows[0].payload["decision"], "needs_manual_match")

    def test_persist_mapping_review_queue_keeps_ready_for_approval_human_gated(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            bootstrap_database(config.db_path)
            item = MappingReviewItem(
                provider="hidive",
                provider_series_id="season-2",
                title="Example Show",
                season_title="Example Show Season 2",
                existing_mapping=None,
                suggested_mal_anime_id=222,
                suggested_mal_title="Example Show Season 2",
                mapping_status="strong",
                confidence=0.95,
                decision="ready_for_approval",
                reasons=["strong_exact_season_candidate"],
            )

            result = persist_mapping_review_queue(config, [item])
            rows = list_review_queue_entries(config.db_path, status="open", issue_type="mapping_review")
            mappings = list_series_mappings(config.db_path, provider="hidive")

        self.assertEqual({"resolved": 0, "inserted": 1}, result)
        self.assertEqual(["season-2"], [row.provider_series_id for row in rows])
        self.assertEqual("ready_for_approval", rows[0].payload["decision"])
        self.assertEqual([], mappings)

    def test_persist_sync_review_queue_keeps_review_and_skip_rows(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"][0]["completion_ratio"] = 0.95
            ingest_snapshot_payload(payload, config)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
            (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 123,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 123,
                    "title": "Example Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "completed", "num_episodes_watched": 12},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)
            persist_sync_review_queue(config, proposals)
            rows = list_review_queue_entries(config.db_path, status="open", issue_type="sync_review")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].payload["decision"], "skip")

    def test_sync_review_queue_preserves_multi_provider_identity(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            bootstrap_database(config.db_path)

            proposals = [
                SyncProposal(
                    provider_series_id="cr-series-123",
                    provider_title="Crunchyroll Review Show",
                    mapping_status="no_candidates",
                    confidence=0.0,
                    mal_anime_id=None,
                    mal_title=None,
                    current_my_list_status=None,
                    proposed_my_list_status=None,
                    decision="review",
                    reasons=["no_candidates"],
                    provider="crunchyroll",
                ),
                SyncProposal(
                    provider_series_id="hidive-series-123",
                    provider_title="HIDIVE Review Show",
                    mapping_status="no_candidates",
                    confidence=0.0,
                    mal_anime_id=None,
                    mal_title=None,
                    current_my_list_status=None,
                    proposed_my_list_status=None,
                    decision="review",
                    reasons=["no_candidates"],
                    provider="hidive",
                ),
            ]

            self.assertEqual("crunchyroll", proposals[0].as_dict()["provider"])
            self.assertEqual("hidive", proposals[1].as_dict()["provider"])
            self.assertEqual("HIDIVE Review Show", proposals[1].as_dict()["crunchyroll_title"])

            persist_sync_review_queue(config, proposals)
            rows = list_review_queue_entries(config.db_path, status="open", issue_type="sync_review")

        providers_by_series_id = {row.provider_series_id: row.provider for row in rows}
        payload_providers_by_series_id = {row.provider_series_id: row.payload["provider"] for row in rows}
        self.assertEqual(
            {"cr-series-123": "crunchyroll", "hidive-series-123": "hidive"},
            providers_by_series_id,
        )
        self.assertEqual(providers_by_series_id, payload_providers_by_series_id)

    def test_build_dry_run_sync_plan_provider_all_keeps_crunchyroll_and_hidive_providers(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            crunchyroll_payload = sample_snapshot()
            crunchyroll_payload["series"][0]["provider_series_id"] = "crunchyroll-series-123"
            crunchyroll_payload["progress"][0]["provider_series_id"] = "crunchyroll-series-123"
            crunchyroll_payload["watchlist"][0]["provider_series_id"] = "crunchyroll-series-123"
            hidive_payload = copy.deepcopy(sample_snapshot())
            hidive_payload["provider"] = "hidive"
            hidive_payload["series"][0]["provider_series_id"] = "hidive-series-123"
            hidive_payload["series"][0]["title"] = "HIDIVE Example Show"
            hidive_payload["series"][0]["season_title"] = "HIDIVE Example Show Season 1"
            hidive_payload["progress"][0]["provider_series_id"] = "hidive-series-123"
            hidive_payload["progress"][0]["provider_episode_id"] = "hidive-episode-456"
            hidive_payload["watchlist"][0]["provider_series_id"] = "hidive-series-123"
            ingest_snapshot_payload(crunchyroll_payload, config)
            ingest_snapshot_payload(hidive_payload, config)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
            (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(MalClient, "search_anime", return_value={"data": []}):
                proposals = build_dry_run_sync_plan(config, limit=None, mapping_limit=3, provider=None)

        providers_by_series_id = {proposal.provider_series_id: proposal.provider for proposal in proposals}
        self.assertEqual(
            {"crunchyroll-series-123": "crunchyroll", "hidive-series-123": "hidive"},
            providers_by_series_id,
        )
        self.assertEqual({proposal.provider for proposal in proposals}, {payload["provider"] for payload in (proposal.as_dict() for proposal in proposals)})

    def test_build_dry_run_sync_plan_fills_missing_finish_date_only_when_completed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"][0]["episode_number"] = 12
            payload["progress"][0]["completion_ratio"] = 0.95
            payload["progress"][0]["last_watched_at"] = "2026-03-14T22:10:00Z"
            ingest_snapshot_payload(payload, config)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
            (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 123,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 123,
                    "title": "Example Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "completed", "num_episodes_watched": 12, "finish_date": None},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)

        self.assertEqual(proposals[0].decision, "propose_update")
        self.assertEqual(
            proposals[0].proposed_my_list_status,
            {"status": "completed", "num_watched_episodes": 12, "finish_date": "2026-03-14"},
        )
        self.assertIn("fill_missing_finish_date", proposals[0].reasons)
        self.assertIn("preserve_meaningful_start_date", proposals[0].reasons)

    def test_build_dry_run_sync_plan_preserves_existing_finish_date(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"][0]["episode_number"] = 12
            payload["progress"][0]["completion_ratio"] = 0.95
            payload["progress"][0]["last_watched_at"] = "2026-03-14T22:10:00Z"
            ingest_snapshot_payload(payload, config)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
            (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 123,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 123,
                    "title": "Example Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "completed", "num_episodes_watched": 12, "finish_date": "2025-01-01"},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)

        self.assertEqual(proposals[0].decision, "skip")
        self.assertTrue(any(reason == "mal_already_matches_or_exceeds_proposal" for reason in proposals[0].reasons))

    def test_build_dry_run_sync_plan_preserves_meaningful_zero_progress_on_plan_to_watch(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"] = []
            ingest_snapshot_payload(payload, config)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
            (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 123,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 123,
                    "title": "Example Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "plan_to_watch", "num_episodes_watched": 0},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)

        self.assertEqual(proposals[0].decision, "skip")
        self.assertIn("mal_already_matches_or_exceeds_proposal", proposals[0].reasons)

    def test_build_dry_run_sync_plan_overrides_plan_to_watch_when_crunchyroll_has_completed_episode_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"][0]["episode_number"] = 2
            payload["progress"][0]["completion_ratio"] = 0.95
            ingest_snapshot_payload(payload, config)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
            (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 123,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 123,
                    "title": "Example Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "plan_to_watch", "num_episodes_watched": 0},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)

        self.assertEqual(proposals[0].decision, "propose_update")
        self.assertEqual(proposals[0].proposed_my_list_status, {"status": "watching", "num_watched_episodes": 2})
        self.assertIn("override_plan_to_watch_due_to_provider_watch_evidence", proposals[0].reasons)

    def test_build_dry_run_sync_plan_suppresses_watching_zero_episode_proposals(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"][0]["completion_ratio"] = 0.40
            payload["progress"][0]["episode_number"] = 1
            ingest_snapshot_payload(payload, config)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
            (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 123,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 123,
                    "title": "Example Show",
                    "num_episodes": 12,
                    "my_list_status": None,
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)

        self.assertEqual(proposals[0].decision, "skip")
        self.assertIsNone(proposals[0].proposed_my_list_status)
        self.assertIn("partial_provider_activity_without_completed_episode", proposals[0].reasons)
        self.assertIn("no_actionable_provider_state", proposals[0].reasons)

    def test_build_dry_run_sync_plan_counts_follow_on_near_complete_episode_as_watched(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"] = [
                {
                    **payload["progress"][0],
                    "provider_episode_id": "episode-1",
                    "episode_number": 1,
                    "playback_position_ms": 1300000,
                    "duration_ms": 1440024,
                    "completion_ratio": 0.9027610239707948,
                    "last_watched_at": "2026-03-14T20:00:00Z",
                },
                {
                    **payload["progress"][0],
                    "provider_episode_id": "episode-2",
                    "episode_number": 2,
                    "playback_position_ms": 1440000,
                    "duration_ms": 1440066,
                    "completion_ratio": 0.9999541687672648,
                    "last_watched_at": "2026-03-14T20:30:00Z",
                },
            ]
            ingest_snapshot_payload(payload, config)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
            (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 123,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 123,
                    "title": "Example Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "watching", "num_episodes_watched": 0},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)

        self.assertEqual(proposals[0].decision, "propose_update")
        self.assertEqual(proposals[0].proposed_my_list_status, {"status": "watching", "num_watched_episodes": 2})
        self.assertIn("completion_policy=ratio>=0.95_or_remaining<=120s_or_later_episode_progress_with_ratio>=0.85", proposals[0].reasons)
        self.assertEqual(1, proposals[0].completion_audit["completed_by"]["later_episode_evidence"])
        self.assertEqual(1, proposals[0].completion_audit["completed_by"]["ratio_threshold"])
        self.assertIn("ep1@ratio=0.903", proposals[0].completion_audit["completed_examples"]["later_episode_evidence"][0])

    def test_sync_proposal_as_dict_includes_completion_audit(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"][0]["episode_number"] = 12
            payload["progress"][0]["completion_ratio"] = 0.95
            ingest_snapshot_payload(payload, config)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
            (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 123,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 123,
                    "title": "Example Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "watching", "num_episodes_watched": 0},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)

        payload_dict = proposals[0].as_dict()
        self.assertIn("completion_audit", payload_dict)
        self.assertEqual(1, payload_dict["completion_audit"]["completed_by"]["ratio_threshold"])
        self.assertEqual([], payload_dict["completion_audit"]["incomplete_examples"])

    def test_build_dry_run_sync_plan_counts_last_episode_within_credits_window_as_completed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"][0]["episode_number"] = 12
            payload["progress"][0]["playback_position_ms"] = 1322000
            payload["progress"][0]["duration_ms"] = 1440024
            payload["progress"][0]["completion_ratio"] = 0.9180402548846408
            ingest_snapshot_payload(payload, config)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
            (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 123,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 123,
                    "title": "Example Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "watching", "num_episodes_watched": 11, "finish_date": None},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)

        self.assertEqual(proposals[0].decision, "propose_update")
        self.assertEqual(
            proposals[0].proposed_my_list_status,
            {"status": "completed", "num_watched_episodes": 12, "finish_date": "2026-03-14"},
        )

    def test_build_dry_run_sync_plan_leaves_ambiguous_near_complete_episode_incomplete(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"] = [
                {
                    **payload["progress"][0],
                    "provider_episode_id": "episode-6",
                    "episode_number": 6,
                    "playback_position_ms": 1440000,
                    "duration_ms": 1440066,
                    "completion_ratio": 0.9999541687672648,
                    "last_watched_at": "2026-03-14T19:00:00Z",
                },
                {
                    **payload["progress"][0],
                    "provider_episode_id": "episode-7",
                    "episode_number": 7,
                    "playback_position_ms": 1280000,
                    "duration_ms": 1420046,
                    "completion_ratio": 0.9013792510946829,
                    "last_watched_at": "2026-03-14T20:00:00Z",
                },
            ]
            ingest_snapshot_payload(payload, config)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
            (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 123,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 123,
                    "title": "Example Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "watching", "num_episodes_watched": 0},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)

        self.assertEqual(proposals[0].decision, "propose_update")
        self.assertEqual(proposals[0].proposed_my_list_status, {"status": "watching", "num_watched_episodes": 6})

    def test_build_dry_run_sync_plan_deduplicates_alternate_episode_variants_by_episode_number(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"] = [
                {
                    **payload["progress"][0],
                    "provider_episode_id": "episode-1-dub-a",
                    "episode_number": 1,
                    "playback_position_ms": 1440000,
                    "duration_ms": 1440066,
                    "completion_ratio": 0.9999541687672648,
                    "last_watched_at": "2026-03-14T18:00:00Z",
                },
                {
                    **payload["progress"][0],
                    "provider_episode_id": "episode-1-dub-b",
                    "episode_number": 1,
                    "playback_position_ms": 1440000,
                    "duration_ms": 1440066,
                    "completion_ratio": 0.9999541687672648,
                    "last_watched_at": "2026-03-14T18:05:00Z",
                },
                {
                    **payload["progress"][0],
                    "provider_episode_id": "episode-2-dub-a",
                    "episode_number": 2,
                    "playback_position_ms": 1440000,
                    "duration_ms": 1440066,
                    "completion_ratio": 0.9999541687672648,
                    "last_watched_at": "2026-03-14T18:30:00Z",
                },
            ]
            ingest_snapshot_payload(payload, config)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
            (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 123,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 123,
                    "title": "Example Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "watching", "num_episodes_watched": 0},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)

        self.assertEqual(proposals[0].decision, "propose_update")
        self.assertEqual(proposals[0].proposed_my_list_status, {"status": "watching", "num_watched_episodes": 2})

    def test_execute_approved_sync_dry_run_only_targets_approved_safe_updates(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"][0]["episode_number"] = 4
            payload["progress"][0]["completion_ratio"] = 0.95
            ingest_snapshot_payload(payload, config)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
            (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")
            upsert_series_mapping(
                config.db_path,
                provider="crunchyroll",
                provider_series_id="series-123",
                mal_anime_id=888,
                confidence=1.0,
                mapping_source="user_approved",
                approved_by_user=True,
                notes=None,
            )

            with patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 888,
                    "title": "Approved Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "watching", "num_episodes_watched": 2, "score": 9},
                },
            ), patch.object(MalClient, "update_my_list_status", side_effect=AssertionError("dry-run should not write")):
                results = execute_approved_sync(config, limit=5, dry_run=True)

        self.assertEqual(len(results), 1)
        self.assertFalse(results[0].applied)
        self.assertEqual(results[0].proposal_decision, "propose_update")
        self.assertEqual(results[0].requested_status, {"status": "watching", "num_watched_episodes": 4})
        self.assertIn("executor_dry_run", results[0].reasons)

    def test_execute_approved_sync_performs_live_write_when_safe(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"][0]["episode_number"] = 4
            payload["progress"][0]["completion_ratio"] = 0.95
            ingest_snapshot_payload(payload, config)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
            (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")
            upsert_series_mapping(
                config.db_path,
                provider="crunchyroll",
                provider_series_id="series-123",
                mal_anime_id=888,
                confidence=1.0,
                mapping_source="user_approved",
                approved_by_user=True,
                notes=None,
            )

            with patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 888,
                    "title": "Approved Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "watching", "num_episodes_watched": 2, "score": 9},
                },
            ), patch.object(
                MalClient,
                "update_my_list_status",
                return_value={"status": "watching", "num_episodes_watched": 4, "score": 9},
            ) as update_mock:
                results = execute_approved_sync(config, limit=5, dry_run=False)

        self.assertEqual(len(results), 1)
        self.assertTrue(results[0].applied)
        update_mock.assert_called_once_with(888, status="watching", num_watched_episodes=4, score=None, start_date=None, finish_date=None)
        self.assertEqual(results[0].response_status["score"], 9)

    def test_execute_approved_sync_includes_missing_finish_date_when_safe(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"][0]["episode_number"] = 12
            payload["progress"][0]["completion_ratio"] = 0.95
            payload["progress"][0]["last_watched_at"] = "2026-03-14T22:10:00Z"
            ingest_snapshot_payload(payload, config)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
            (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")
            upsert_series_mapping(
                config.db_path,
                provider="crunchyroll",
                provider_series_id="series-123",
                mal_anime_id=888,
                confidence=1.0,
                mapping_source="user_approved",
                approved_by_user=True,
                notes=None,
            )

            with patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 888,
                    "title": "Approved Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "completed", "num_episodes_watched": 12, "finish_date": None, "score": 9},
                },
            ), patch.object(
                MalClient,
                "update_my_list_status",
                return_value={"status": "completed", "num_episodes_watched": 12, "finish_date": "2026-03-14", "score": 9},
            ) as update_mock:
                results = execute_approved_sync(config, limit=5, dry_run=False)

        self.assertTrue(results[0].applied)
        update_mock.assert_called_once_with(
            888,
            status="completed",
            num_watched_episodes=12,
            score=None,
            start_date=None,
            finish_date="2026-03-14",
        )
        self.assertEqual(results[0].requested_status["finish_date"], "2026-03-14")

    def test_execute_approved_sync_skips_non_forward_safe_completed_downgrade(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"][0]["episode_number"] = 3
            payload["progress"][0]["completion_ratio"] = 0.95
            ingest_snapshot_payload(payload, config)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True, exist_ok=True)
            (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")
            upsert_series_mapping(
                config.db_path,
                provider="crunchyroll",
                provider_series_id="series-123",
                mal_anime_id=888,
                confidence=1.0,
                mapping_source="user_approved",
                approved_by_user=True,
                notes=None,
            )

            with patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 888,
                    "title": "Approved Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "completed", "num_episodes_watched": 12},
                },
            ), patch.object(MalClient, "update_my_list_status", side_effect=AssertionError("unsafe proposal should not write")):
                results = execute_approved_sync(config, limit=5, dry_run=False)

        self.assertEqual(len(results), 1)
        self.assertFalse(results[0].applied)
        self.assertEqual(results[0].proposal_decision, "skip")
        self.assertTrue(any("refusing_to_decrease_mal_progress" in reason or "refusing_to_downgrade_completed_mal_entry" in reason for reason in results[0].reasons))


if __name__ == "__main__":
    unittest.main()
