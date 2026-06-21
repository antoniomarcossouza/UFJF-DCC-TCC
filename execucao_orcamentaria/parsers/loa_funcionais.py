"""Extrai funções e subfunções da LOA PJF (DimLOA, PDFs em Funcionais.zip)."""

from __future__ import annotations

import io
import re
import shutil
import subprocess
import tempfile
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

OCR_PSM_MODES = (4, 6, 11)
EXPECTED_FUNCAO_COUNT = 29
EXPECTED_SUBFUNCAO_COUNT = 117
FUNC_CODE_RE = re.compile(r"^\d{2}$")
SUBF_CODE_RE = re.compile(r"^\d{3}$")
FUNC_LINE_RE = re.compile(r"^(\d{2})\s+-\s+(.+)$")
SUBF_LINE_RE = re.compile(r"^(\d{3})\s+-\s+(.+)$")
FUNC_ROW_RE = re.compile(
    r"^(\d{2})\s+(.+?)\s+(\d{1,3}(?:\.\d{3})*(?:,\d{2})?|0,00)$"
)
SUBF_ROW_RE = re.compile(
    r"^(\d{3})\s+(.+?)\s+(\d{1,3}(?:\.\d{3})*(?:,\d{2})?|0,00)$"
)
MONEY_RE = re.compile(r"^\d{1,3}(?:\.\d{3})*(?:,\d{2})?$|^0,00$")
SKIP_LINE = re.compile(
    r"^(PREFEITURA|Data:|Hora:|Exercício:|Gestão Orçamentária|"
    r"Relação de Funções|Relação de Sub-Funções|Página:|"
    r"Código|Descrição|Despesa|Registro\(s\):|Elaborado|DimLOA|9itl)$",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class FuncaoRow:
    cd_funcao: str
    ds_funcao: str


@dataclass(frozen=True, slots=True)
class SubfuncaoRow:
    cd_subfuncao: str
    ds_subfuncao: str


def tesseract_available() -> bool:
    return shutil.which("tesseract") is not None


def pdftotext_available() -> bool:
    return shutil.which("pdftotext") is not None


def pdftoppm_available() -> bool:
    return shutil.which("pdftoppm") is not None


def _require_binary(name: str) -> None:
    if shutil.which(name) is None:
        msg = (
            f"{name} não encontrado no PATH; "
            "instale poppler-utils e tesseract-ocr "
            "(ver references/LOA_PJF.md)"
        )
        raise RuntimeError(msg)


def pdf_to_text(pdf_path: Path) -> str:
    """Extrai texto do PDF via pdftotext (poppler)."""
    _require_binary("pdftotext")
    result = subprocess.run(
        ["pdftotext", str(pdf_path), "-"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        msg = (
            f"pdftotext falhou para {pdf_path}: "
            f"{result.stderr.strip() or result.returncode}"
        )
        raise RuntimeError(msg)
    return result.stdout


def pdf_to_ocr_text(
    pdf_path: Path,
    lang: str = "por",
    *,
    psm: int = 6,
) -> str:
    """OCR página a página (pdftoppm + tesseract) para PDFs escaneados."""
    _require_binary("pdftoppm")
    if not tesseract_available():
        msg = (
            "tesseract não encontrado no PATH; "
            "instale tesseract-ocr e tesseract-ocr-por"
        )
        raise RuntimeError(msg)

    with tempfile.TemporaryDirectory() as tmp:
        prefix = Path(tmp) / "page"
        result = subprocess.run(
            ["pdftoppm", "-png", str(pdf_path), str(prefix)],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            msg = (
                f"pdftoppm falhou para {pdf_path}: "
                f"{result.stderr.strip() or result.returncode}"
            )
            raise RuntimeError(msg)

        pages = sorted(Path(tmp).glob("page*.png"))
        chunks: list[str] = []
        for page in pages:
            ocr = subprocess.run(
                [
                    "tesseract",
                    str(page),
                    "stdout",
                    "-l",
                    lang,
                    "--psm",
                    str(psm),
                    "--oem",
                    "1",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            if ocr.returncode != 0:
                msg = (
                    f"tesseract falhou para {page}: "
                    f"{ocr.stderr.strip() or ocr.returncode}"
                )
                raise RuntimeError(msg)
            chunks.append(ocr.stdout)
        return "\n".join(chunks)


def _parse_pdf_with_ocr[T](
    pdf_path: Path,
    parse_fn: Callable[[str], list[T]],
    expected_count: int,
) -> list[T]:
    """Tenta vários modos OCR e escolhe o parse com mais registros."""
    best_rows: list[T] = []
    for psm in OCR_PSM_MODES:
        text = pdf_to_ocr_text(pdf_path, psm=psm)
        rows = parse_fn(text)
        if len(rows) > len(best_rows):
            best_rows = rows

    min_ok = max(1, int(expected_count * 0.85))
    if len(best_rows) < min_ok:
        msg = (
            f"OCR de {pdf_path.name} extraiu {len(best_rows)} registros; "
            f"esperado pelo menos {min_ok} (LOA ~{expected_count}). "
            "Verifique poppler-utils e tesseract-ocr-por no container."
        )
        raise RuntimeError(msg)
    return best_rows


def pdf_to_lines(pdf_path: Path) -> list[str]:
    """Texto nativo ou OCR; retorna linhas não vazias."""
    if not pdftotext_available() and not tesseract_available():
        _require_binary("pdftotext")

    text = ""
    if pdftotext_available():
        text = pdf_to_text(pdf_path).strip()
    if not text:
        text = pdf_to_ocr_text(pdf_path)
    return [ln.strip() for ln in text.splitlines() if ln.strip()]


def _is_money(line: str) -> bool:
    cleaned = line.replace(" ", "")
    if MONEY_RE.match(cleaned):
        return True
    # OCR ruidoso: 47.495,679,93 ou 4.585.516,873,17
    return bool(re.match(r"^\d[\d.,]+$", cleaned) and "," in cleaned)


def _collect_description(
    lines: list[str],
    start: int,
    code_re: re.Pattern[str],
) -> tuple[str, int]:
    parts: list[str] = []
    i = start
    while i < len(lines):
        nxt = lines[i]
        if code_re.match(nxt):
            break
        if SKIP_LINE.search(nxt):
            i += 1
            continue
        if _is_money(nxt):
            break
        parts.append(nxt)
        i += 1
    return " ".join(parts).strip(), i


def parse_funcao_text(text: str) -> list[FuncaoRow]:
    """Parseia relatório DimLOA de funções."""
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    rows: list[FuncaoRow] = []
    seen: set[str] = set()
    i = 0

    while i < len(lines):
        line = lines[i]
        i += 1

        m_dash = FUNC_LINE_RE.match(line)
        if m_dash:
            code = m_dash.group(1).zfill(2)
            desc = m_dash.group(2).strip()
        else:
            m_row = FUNC_ROW_RE.match(line)
            if m_row:
                code = m_row.group(1).zfill(2)
                desc = m_row.group(2).strip()
            elif FUNC_CODE_RE.match(line):
                code = line.zfill(2)
                desc, i = _collect_description(lines, i, FUNC_CODE_RE)
            else:
                continue

        if not desc or code in seen or SKIP_LINE.search(code):
            continue
        seen.add(code)
        rows.append(FuncaoRow(cd_funcao=code, ds_funcao=desc))

    return rows


def parse_subfuncao_text(text: str) -> list[SubfuncaoRow]:
    """Parseia relatório DimLOA de subfunções."""
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    rows: list[SubfuncaoRow] = []
    seen: set[str] = set()
    i = 0

    while i < len(lines):
        line = lines[i]
        i += 1

        m_dash = SUBF_LINE_RE.match(line)
        if m_dash:
            code = m_dash.group(1).zfill(3)
            desc = m_dash.group(2).strip()
        else:
            m_row = SUBF_ROW_RE.match(line)
            if m_row:
                code = m_row.group(1).zfill(3)
                desc = m_row.group(2).strip()
            elif SUBF_CODE_RE.match(line):
                code = line.zfill(3)
                desc, i = _collect_description(lines, i, SUBF_CODE_RE)
            else:
                continue

        if not desc or code in seen or SKIP_LINE.search(code):
            continue
        seen.add(code)
        rows.append(SubfuncaoRow(cd_subfuncao=code, ds_subfuncao=desc))

    return rows


def parse_funcao_pdf(pdf_path: Path) -> list[FuncaoRow]:
    if pdftotext_available():
        text = pdf_to_text(pdf_path).strip()
        if text:
            rows = parse_funcao_text(text)
            if len(rows) >= int(EXPECTED_FUNCAO_COUNT * 0.85):
                return rows
    return _parse_pdf_with_ocr(
        pdf_path,
        parse_funcao_text,
        EXPECTED_FUNCAO_COUNT,
    )


def parse_subfuncao_pdf(pdf_path: Path) -> list[SubfuncaoRow]:
    if pdftotext_available():
        text = pdf_to_text(pdf_path).strip()
        if text:
            rows = parse_subfuncao_text(text)
            if len(rows) >= int(EXPECTED_SUBFUNCAO_COUNT * 0.85):
                return rows
    return _parse_pdf_with_ocr(
        pdf_path,
        parse_subfuncao_text,
        EXPECTED_SUBFUNCAO_COUNT,
    )


def _find_pdf_in_dir(directory: Path, stem: str) -> Path:
    matches = [
        p
        for p in directory.iterdir()
        if p.suffix.lower() == ".pdf" and p.stem.lower() == stem.lower()
    ]
    if not matches:
        msg = f"{stem}.pdf não encontrado em {directory}"
        raise FileNotFoundError(msg)
    return matches[0]


def extract_funcionais_zip(
    zip_bytes: bytes,
    extract_dir: Path | None = None,
) -> tuple[list[FuncaoRow], list[SubfuncaoRow]]:
    """Extrai Funcionais.zip e parseia Funcao.pdf + SubFuncao.pdf."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            zf.extractall(root)

        if extract_dir is not None:
            extract_dir.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                zf.extractall(extract_dir)

        pdf_root = root
        nested = list(root.glob("**/Funcao.pdf")) + list(
            root.glob("**/funcao.pdf")
        )
        if nested:
            pdf_root = nested[0].parent

        funcao_pdf = _find_pdf_in_dir(pdf_root, "Funcao")
        subfuncao_pdf = _find_pdf_in_dir(pdf_root, "SubFuncao")

        return (
            parse_funcao_pdf(funcao_pdf),
            parse_subfuncao_pdf(subfuncao_pdf),
        )
