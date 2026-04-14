import threading
from pathlib import Path
from typing import Dict, List, Optional


class DiseasePdfCatalog:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.uploads_dir = base_dir / "data" / "uploads"
        self.reference_pdfs_dir = base_dir / "assets" / "reference_pdfs"
        self._cache: Optional[Dict[str, str]] = None
        self._pdf_index_cache: Optional[Dict[str, str]] = None
        self._lock = threading.RLock()

    def invalidate(self) -> None:
        with self._lock:
            self._cache = None
            self._pdf_index_cache = None

    def _iter_pdf_files(self):
        for pdf_file in sorted(self.base_dir.glob("*.pdf")):
            yield pdf_file
        if self.uploads_dir.exists():
            for pdf_file in sorted(self.uploads_dir.glob("*.pdf")):
                yield pdf_file
        if self.reference_pdfs_dir.exists():
            for pdf_file in sorted(self.reference_pdfs_dir.rglob("*.pdf")):
                yield pdf_file

    def _build_pdf_index(self) -> Dict[str, str]:
        index: Dict[str, str] = {}
        for pdf_file in self._iter_pdf_files():
            rel_path = pdf_file.relative_to(self.base_dir).as_posix()
            # Resolve by filename (legacy behavior) and by relative path.
            index.setdefault(pdf_file.name, rel_path)
            index.setdefault(rel_path, rel_path)
        return index

    def _resolve_pdf_reference(self, ref: Optional[str]) -> Optional[str]:
        if not ref:
            return None
        with self._lock:
            if self._pdf_index_cache is None:
                self._pdf_index_cache = self._build_pdf_index()
            return self._pdf_index_cache.get(ref)

    def get_map(self, agent) -> Dict[str, str]:
        with self._lock:
            if self._cache is not None:
                return dict(self._cache)

        mapping: Dict[str, str] = {}
        try:
            with agent.driver.session() as session:
                result = session.run(
                    """
                    MATCH (p:Protocol)-[:COVERS_DISEASE]->(d:Disease)
                    RETURN d.name AS disease, p.name AS protocol
                    """
                )
                for record in result:
                    protocol = record["protocol"] or ""
                    if "TMH" in protocol:
                        mapping[record["disease"]] = "7._HD_CD_TMH.pdf"
                    elif "Dengue" in protocol or "xuất huyết" in protocol:
                        mapping[record["disease"]] = (
                            "_data_soytehcm_trungtamytehocmon_attachments_2023_7_quyet_dinh_2760-qd-byt-2023-"
                            "_huong_dan_chan_doan_dieu_tri_sxh_dengue_19720238.pdf"
                        )
                    elif "Viêm gan" in protocol or "gan" in protocol.lower():
                        mapping[record["disease"]] = (
                            "Hướng dẫn điều trị viêm gan virus B của Bộ Y tế 2019.pdf"
                        )

                result = session.run(
                    """
                    MATCH (d:Disease)<-[:ABOUT_DISEASE]-(:Chunk)
                    WHERE NOT (d)<-[:COVERS_DISEASE]-(:Protocol)
                    RETURN DISTINCT d.name AS disease
                    """
                )
                for record in result:
                    disease_name = record["disease"]
                    if disease_name in mapping:
                        continue
                    disease_name_lower = disease_name.lower()
                    if "dengue" in disease_name_lower or "xuất huyết" in disease_name_lower:
                        mapping[disease_name] = (
                            "_data_soytehcm_trungtamytehocmon_attachments_2023_7_quyet_dinh_2760-qd-byt-2023-"
                            "_huong_dan_chan_doan_dieu_tri_sxh_dengue_19720238.pdf"
                        )
                    elif "gan" in disease_name_lower:
                        mapping[disease_name] = (
                            "Hướng dẫn điều trị viêm gan virus B của Bộ Y tế 2019.pdf"
                        )
        except Exception:
            pass

        with self._lock:
            self._cache = dict(mapping)
            return dict(self._cache)

    def find_pdf_for_disease(self, disease_name: Optional[str], agent) -> Optional[str]:
        if not disease_name:
            return None
        mapping = self.get_map(agent)
        mapped_ref = mapping.get(disease_name)
        return self._resolve_pdf_reference(mapped_ref)

    def list_pdfs(self) -> List[dict]:
        pdfs: List[dict] = []
        seen_paths = set()
        for pdf_file in self._iter_pdf_files():
            rel_path = pdf_file.relative_to(self.base_dir).as_posix()
            if rel_path in seen_paths:
                continue
            seen_paths.add(rel_path)
            pdfs.append(
                {
                    "filename": pdf_file.name,
                    "path": rel_path,
                    "size_mb": round(pdf_file.stat().st_size / 1024 / 1024, 1),
                }
            )
        return pdfs
