from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..ai_host_adapter import AIHostAdapter
from ..ai_missing_episode_support import MissingEpisodeResult, format_episode_labels, parse_missing_episode_request
from ..ai_tool_base import AIToolBase, CommandReply
from ..media_identity_service import MediaIdentityService
from ..telegram_message_renderer import TelegramMessageRenderer

if TYPE_CHECKING:
    from ..telegram_commands import TelegramCommandService


class MissingEpisodeTool(AIToolBase):
    def __init__(
        self,
        host: "AIHostAdapter | TelegramCommandService",
        *,
        conversation_key: str = "",
        chat_id: str = "",
        rich: bool = False,
        name: str = "query_missing_episodes",
        description: str = "查询 Emby 实际单集、每季状态和缺失集。",
        schema: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            name=name,
            description=description,
            schema=schema
            or {
                "type": "object",
                "properties": {"question": {"type": "string"}},
                "required": ["question"],
            },
            kind="read",
        )
        self.host = AIHostAdapter.coerce(host)
        self.conversation_key = str(conversation_key or "").strip()
        self.chat_id = str(chat_id or "").strip()
        self.rich = bool(rich)

    def invoke(self, question: str) -> CommandReply:
        request = parse_missing_episode_request(question)
        if not request:
            return ""
        resolved = self._resolve_identity(question, request=request)
        if "reply" in resolved:
            return resolved["reply"]
        return self._run_query(
            question,
            identity=resolved["identity"],
            keyword=resolved["keyword"],
            search_count=int(resolved["search_count"]),
        )

    def invoke_from_identity(
        self,
        question: str,
        *,
        identity: dict[str, Any],
        search_count: int = 0,
        candidates: list[dict[str, Any]] | None = None,
        forced_confidence_cap: str = "",
        identity_note: str = "",
    ) -> CommandReply:
        del candidates
        keyword = str(identity.get("title") or "").strip() or "未知作品"
        return self._run_query(
            question,
            identity=identity,
            keyword=keyword,
            search_count=search_count,
            forced_confidence_cap=forced_confidence_cap,
            identity_note=identity_note,
        )

    def _resolve_identity(self, question: str, *, request: dict[str, str]) -> dict[str, Any]:
        active = self.host.conversations.get_active_media(self.conversation_key)
        use_active = request.get("mode") == "context"
        keyword = str(active.get("title") if use_active else request.get("title") or "").strip()
        request_year = str(request.get("year") or active.get("year") or "").strip() if use_active else str(request.get("year") or "").strip()
        if not keyword:
            return {"reply": "当前没有记住正在讨论的剧集，请带上片名。"}

        service = self.host.media_service.media_identity_service()
        search_count = 0
        target_identity: dict[str, Any] = {}
        if use_active and active.get("tmdbId"):
            target_identity = {
                "title": keyword,
                "year": request_year,
                "type": "series",
                "tmdbId": str(active.get("tmdbId") or ""),
                "embyId": str(active.get("embySeriesId") or ""),
            }
        else:
            try:
                tmdb_candidates = service.search_media(keyword, media_type="tv")
            except Exception as err:
                return {"reply": f"TMDB 搜索《{keyword}》失败：{self.host.media_service.format_emby_error(err)}"}
            if not tmdb_candidates:
                return {"reply": f"TMDB 没有找到《{keyword}》，请先确认作品身份。"}
            tmdb_candidates = self._prefer_exact_title_candidates(keyword, tmdb_candidates)
            if request_year:
                year_matched = [row for row in tmdb_candidates if str(row.get("year") or "").strip() == request_year]
                if year_matched:
                    tmdb_candidates = year_matched + [row for row in tmdb_candidates if row not in year_matched]
            if self._is_tmdb_candidate_ambiguous(tmdb_candidates):
                tmdb_hits = self._query_tmdb_candidates_in_library(
                    service=service,
                    keyword=keyword,
                    request_year=request_year,
                    candidates=tmdb_candidates,
                )
                if len(tmdb_hits) == 1:
                    target_identity = dict(tmdb_hits[0])
                    search_count = 1
                else:
                    narrowed = tmdb_hits or tmdb_candidates
                    return {"reply": self.host.media.format_ai_identity_candidates(keyword, narrowed)}
            else:
                target_identity = dict(tmdb_candidates[0])
                search_count = 1

        target_tmdb = str(target_identity.get("tmdbId") or "").strip()
        if not target_tmdb:
            return {"reply": f"还没有确认《{keyword}》的 TMDB 身份，请先确认作品身份。"}

        exact_lookup = getattr(service, "query_library_exists_by_tmdb", None)
        if not callable(exact_lookup):
            exact_lookup = getattr(service, "query_library_exists")
        exact_inventory = exact_lookup(
            {
                "title": str(target_identity.get("title") or keyword).strip(),
                "year": str(target_identity.get("year") or request_year or "").strip(),
                "type": "series",
                "tmdbId": target_tmdb,
                "embyId": str(target_identity.get("embyId") or "").strip(),
            }
        )
        exact_item = exact_inventory.get("embyItem") if isinstance(exact_inventory.get("embyItem"), dict) else {}
        exact_item_id = str(exact_item.get("Id") or "").strip()
        exact_provider_tmdb = str(
            (
                exact_item.get("ProviderIds")
                if isinstance(exact_item.get("ProviderIds"), dict)
                else {}
            ).get("Tmdb")
            or (
                exact_item.get("ProviderIds")
                if isinstance(exact_item.get("ProviderIds"), dict)
                else {}
            ).get("tmdb")
            or ""
        ).strip()
        if exact_item_id and (not exact_provider_tmdb or exact_provider_tmdb == target_tmdb):
            identity = {
                "title": str(target_identity.get("title") or keyword).strip(),
                "year": str(target_identity.get("year") or request_year or "").strip(),
                "type": "series",
                "tmdbId": target_tmdb,
                "embyId": exact_item_id,
                "forceEmbyItem": True,
            }
            return {"identity": identity, "keyword": keyword, "search_count": search_count}

        search_local_candidates = getattr(service, "search_local_candidates", None)
        if not callable(search_local_candidates):
            if use_active and str(active.get("embySeriesId") or "").strip():
                identity = {
                    "title": str(target_identity.get("title") or keyword).strip(),
                    "year": str(target_identity.get("year") or request_year or "").strip(),
                    "type": "series",
                    "tmdbId": target_tmdb,
                    "embyId": str(active.get("embySeriesId") or "").strip(),
                    "forceEmbyItem": True,
                }
                return {"identity": identity, "keyword": keyword, "search_count": search_count}
            return {
                "reply": (
                    f"没有在 Emby 中找到与 TMDB {target_tmdb} 完全一致的作品。"
                    f" 为了避免把别的剧当成《{target_identity.get('title') or keyword}》来算缺集，这里先不继续计算。"
                )
            }

        local_candidates = search_local_candidates(
            keyword,
            preferred_type="series",
            tmdb_id=target_tmdb,
            year=str(target_identity.get("year") or request_year or ""),
            alternative_titles=[str(target_identity.get("title") or "").strip()],
            limit=8,
        )
        active_emby_id = str(active.get("embySeriesId") or "").strip()
        if use_active and active_emby_id:
            exact_active_candidates = [
                row for row in local_candidates
                if str(row.get("embyItemId") or "").strip() == active_emby_id
            ]
            if len(local_candidates) == 1 and len(exact_active_candidates) == 1:
                identity = {
                    "title": str(target_identity.get("title") or keyword).strip(),
                    "year": str(target_identity.get("year") or request_year or "").strip(),
                    "type": "series",
                    "tmdbId": target_tmdb,
                    "embyId": active_emby_id,
                    "forceEmbyItem": True,
                }
                return {"identity": identity, "keyword": keyword, "search_count": search_count}
        if not local_candidates:
            return {
                "reply": (
                    f"没有在 Emby 中找到与 TMDB {target_tmdb} 完全一致的作品。"
                    f" 为了避免把别的剧当成《{target_identity.get('title') or keyword}》来算缺集，这里先不继续计算。"
                )
            }
        if not self._should_confirm_local_candidates(local_candidates, target_identity):
            top = dict(local_candidates[0])
            identity = {
                "title": str(target_identity.get("title") or keyword).strip(),
                "year": str(target_identity.get("year") or request_year or "").strip(),
                "type": "series",
                "tmdbId": target_tmdb,
                "embyId": str(top.get("embyItemId") or "").strip(),
                "forceEmbyItem": True,
            }
            return {"identity": identity, "keyword": keyword, "search_count": search_count}
        return {"reply": self._local_candidates_reply(question, target_identity=target_identity, candidates=local_candidates)}

    @staticmethod
    def _is_tmdb_candidate_ambiguous(candidates: list[dict[str, Any]]) -> bool:
        if len(candidates) < 2:
            return False
        first, second = candidates[0], candidates[1]
        return int(first.get("score") or 0) == int(second.get("score") or 0) and first.get("year") != second.get("year")

    @staticmethod
    def _prefer_exact_title_candidates(keyword: str, candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized_keyword = MediaIdentityService.normalize_title(keyword)
        exact = [
            row for row in candidates
            if MediaIdentityService.normalize_title(str(row.get("title") or "")) == normalized_keyword
        ]
        return exact or candidates

    def _query_tmdb_candidates_in_library(
        self,
        *,
        service: Any,
        keyword: str,
        request_year: str,
        candidates: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        exact_lookup = getattr(service, "query_library_exists_by_tmdb", None)
        if not callable(exact_lookup):
            exact_lookup = getattr(service, "query_library_exists", None)
        if not callable(exact_lookup):
            return []
        hits: list[dict[str, Any]] = []
        for row in candidates[:5]:
            tmdb_id = str(row.get("tmdbId") or "").strip()
            if not tmdb_id:
                continue
            inventory = exact_lookup(
                {
                    "title": str(row.get("title") or keyword).strip(),
                    "year": str(row.get("year") or request_year or "").strip(),
                    "type": "series",
                    "tmdbId": tmdb_id,
                }
            )
            item = inventory.get("embyItem") if isinstance(inventory, dict) and isinstance(inventory.get("embyItem"), dict) else {}
            item_id = str(item.get("Id") or "").strip()
            provider_ids = item.get("ProviderIds") if isinstance(item.get("ProviderIds"), dict) else {}
            provider_tmdb = str(provider_ids.get("Tmdb") or provider_ids.get("tmdb") or "").strip()
            if item_id and (not provider_tmdb or provider_tmdb == tmdb_id):
                hit = dict(row)
                hit["embyId"] = item_id
                hits.append(hit)
        return hits

    @staticmethod
    def _should_confirm_local_candidates(candidates: list[dict[str, Any]], target_identity: dict[str, Any]) -> bool:
        if not candidates:
            return True
        top = candidates[0]
        if len(candidates) > 1:
            return True
        if not bool(top.get("isSeries")):
            return True
        if not bool(top.get("isTitleExact")):
            return True
        provider_tmdb = str(top.get("tmdbId") or "").strip()
        target_tmdb = str(target_identity.get("tmdbId") or "").strip()
        if provider_tmdb and target_tmdb and provider_tmdb != target_tmdb:
            return True
        target_year = str(target_identity.get("year") or "").strip()
        candidate_year = str(top.get("year") or "").strip()
        if target_year and candidate_year and target_year != candidate_year:
            return True
        return False

    def _local_candidates_reply(
        self,
        question: str,
        *,
        target_identity: dict[str, Any],
        candidates: list[dict[str, Any]],
    ) -> CommandReply:
        report = {
            "title": str(target_identity.get("title") or "未知作品").strip(),
            "targetTitle": str(target_identity.get("title") or "未知作品").strip(),
            "targetYear": str(target_identity.get("year") or "").strip(),
            "targetTmdbId": str(target_identity.get("tmdbId") or "").strip(),
            "confidenceReason": (
                f"没有在 Emby 中找到与 TMDB {str(target_identity.get('tmdbId') or '').strip() or '-'} 完全一致的作品。"
                f" 为了避免把别的剧当成《{str(target_identity.get('title') or '未知作品').strip() or '未知作品'}》来算缺集，这里先不继续计算。"
                " 请先从 Emby 本地候选里确认具体条目。"
            ),
            "candidates": [self._normalize_local_candidate_row(row, target_identity=target_identity) for row in candidates],
        }
        if not self.rich:
            return self._local_candidates_text(report)
        action_id = self.host.actions.register_pending_missing_identity(
            {
                "chatId": self.chat_id,
                "conversationKey": self.conversation_key,
                "question": question,
                "targetIdentity": {
                    "title": str(target_identity.get("title") or "").strip(),
                    "year": str(target_identity.get("year") or "").strip(),
                    "type": str(target_identity.get("type") or "series").strip() or "series",
                    "tmdbId": str(target_identity.get("tmdbId") or "").strip(),
                },
                "candidates": report["candidates"],
            }
        )
        report["actionId"] = action_id
        return TelegramMessageRenderer(self.host, chat_id=self.chat_id).missing_episode_identity_mismatch_reply(report)

    @staticmethod
    def _normalize_local_candidate_row(row: dict[str, Any], *, target_identity: dict[str, Any]) -> dict[str, Any]:
        provider_tmdb = str(row.get("tmdbId") or "").strip()
        target_tmdb = str(target_identity.get("tmdbId") or "").strip()
        reason_parts = [str(row.get("scoreReason") or "").strip()]
        if provider_tmdb and target_tmdb and provider_tmdb != target_tmdb:
            reason_parts.append("与目标 TMDB 不一致")
        return {
            "embyItemId": str(row.get("embyItemId") or "").strip(),
            "title": str(row.get("title") or "").strip() or "未知作品",
            "year": str(row.get("year") or "").strip(),
            "type": str(row.get("type") or "series").strip(),
            "tmdbId": provider_tmdb,
            "episodeCount": int(row.get("episodeCount") or 0),
            "scoreReason": " / ".join(part for part in reason_parts if part),
            "isTitleExact": bool(row.get("isTitleExact")),
            "isSeries": bool(row.get("isSeries")),
        }

    @staticmethod
    def _local_candidates_text(report: dict[str, Any]) -> str:
        lines = [
            f"《{report.get('targetTitle') or '未知作品'}》缺集查询需要先确认 Emby 本地条目：",
            f"- 目标年份：{report.get('targetYear') or '年份未知'}",
            f"- 目标 TMDB：{report.get('targetTmdbId') or '-'}",
        ]
        for index, row in enumerate(report.get("candidates", []), start=1):
            if not isinstance(row, dict):
                continue
            lines.append(
                f"{index}. {row.get('title')}（{row.get('year') or '年份未知'}）"
                f" / TMDB {row.get('tmdbId') or '-'} / {row.get('episodeCount') or 0} 集"
            )
        return "\n".join(lines)

    def _run_query(
        self,
        question: str,
        *,
        identity: dict[str, Any],
        keyword: str,
        search_count: int,
        forced_confidence_cap: str = "",
        identity_note: str = "",
    ) -> CommandReply:
        tmdb_id = str(identity.get("tmdbId") or "").strip()
        if not tmdb_id:
            return f"还没有确认《{keyword}》的 TMDB 身份，请先确认作品身份。"

        try:
            result = self.host.media_service.build_missing_episode_result(identity=identity, server="emby")
        except Exception as err:
            return f"查询《{keyword}》缺集失败：{self.host.media_service.format_emby_error(err)}"
        if not result.library_exists:
            return f"TMDB 已识别《{result.title or keyword}》，但 Emby 媒体库中未找到该剧集。"
        if identity_note and not result.identity_note:
            result.identity_note = identity_note
        if forced_confidence_cap:
            current_rank = {"low": 1, "medium": 2, "high": 3}.get(str(result.mapping_confidence or "").strip().lower(), 0)
            cap_value = str(forced_confidence_cap or "").strip().lower()
            cap_rank = {"low": 1, "medium": 2, "high": 3}.get(cap_value, 0)
            if cap_rank and current_rank > cap_rank:
                result.mapping_confidence = cap_value
        result.candidate_count = max(int(result.candidate_count or 0), 1)

        report = result.to_report_dict(
            search_count=search_count,
            data_query_count=result.data_query_count,
        )
        report["memoryText"] = self._memory_text(result)
        report["embyProviderTmdbId"] = str(result.emby_provider_tmdb_id or "")

        self.host.conversations.set_active_media(
            self.conversation_key,
            {
                "title": result.title,
                "year": result.year,
                "type": "series",
                "tmdbId": result.tmdb_id,
                "embySeriesId": result.emby_item_id or str(identity.get("embyId") or ""),
                "actualEpisodeCount": result.existing_episodes,
                "expectedEpisodeCount": result.aired_episodes,
                "missingEpisodeCount": len(result.missing_episodes if result.reliable else result.reference_missing_episodes),
                "seasonCount": len(result.seasons),
            },
        )

        renderer = TelegramMessageRenderer(self.host, chat_id=self.chat_id)
        rich_reply = renderer.missing_episode_report_reply(report)
        return rich_reply if self.rich else str(rich_reply.get("memory_text") or rich_reply.get("fallback_text") or "")

    @staticmethod
    def _memory_text(result: MissingEpisodeResult) -> str:
        lines = [
            f"《{result.title}》缺集查询结果：",
            f"- TMDB：已播出 {result.aired_episodes} 集，登记 {result.registered_episodes or result.total_episodes} 集",
            f"- Emby：实际找到 {result.existing_episodes} 集，成功映射 {result.mapped_episodes} 集，未映射 {result.unmapped_episodes} 集",
            f"- 缺集判断：{'可靠' if result.reliable else '不可靠'}",
        ]
        if result.identity_note:
            lines.append(f"- 身份说明：{result.identity_note}")
        if result.missing_source:
            source_text = "Emby 自带缺失数据" if result.missing_source == "emby_missing" else "TMDB 已播集对比"
            lines.append(f"- 缺集来源：{source_text}")
        if result.mapping_warning:
            lines.append(f"- 映射警告：{result.mapping_warning}")
        if result.total_episodes <= 0 and result.reference_missing_episodes:
            lines.append(f"- 本地内部断档：{format_episode_labels(result.reference_missing_episodes)}")
        season_lines = [
            result.seasons[season].summary_line(
                reliable=result.reliable,
                identity_note=result.identity_note,
                mapping_warning=result.mapping_warning,
            )
            for season in sorted(result.seasons)
        ]
        lines.extend(season_lines[:12])
        return "\n".join(lines)
