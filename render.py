#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
자막 여러 개를 한 번에 투명 PNG로 그려서 저장함(배치 처리).
생성(마지막 작업): 2026-09-06 21:20 (KST)

[2026-09-06 21:20] 최적화 — 예전엔 자막 문장 하나마다 파이썬 프로세스를 새로 띄웠는데(영상 하나에
문장이 수십~백개라 그만큼 프로세스 생성 오버헤드가 누적됨), 이제 영상 하나(또는 청크 하나) 분량의
자막 요청을 JSON 매니페스트 파일 하나로 모아서 받고, 파이썬 프로세스 1번 안에서 전부 처리함.
같은 폰트+크기 조합은 캐싱해서 재사용 — 폰트 파일 읽기/파싱 비용도 반복 안 되게 함.

사용법: python3 render_caption.py <매니페스트.json>
매니페스트는 아래 필드를 가진 객체의 배열:
  text_file, font_file, font_size, color_hex, canvas_w, canvas_h,
  x, y_mode(top|bottom|middle), y_offset, stroke_width, stroke_color, spacing, out_path
"""
import sys
import json


def render_one(Image, ImageDraw, ImageFont, font_cache, job):
    key = (job["font_file"], job["font_size"])
    font = font_cache.get(key)
    if font is None:
        font = ImageFont.truetype(job["font_file"], job["font_size"])
        font_cache[key] = font

    with open(job["text_file"], "r", encoding="utf-8") as f:
        text = f.read()

    canvas_w, canvas_h = job["canvas_w"], job["canvas_h"]
    img = Image.new("RGBA", (canvas_w, canvas_h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    spacing = job["spacing"]
    stroke_width = job["stroke_width"]
    bbox = draw.multiline_textbbox((0, 0), text, font=font, spacing=spacing, stroke_width=stroke_width)
    text_h = bbox[3] - bbox[1]

    y_mode, y_offset, x = job["y_mode"], job["y_offset"], job["x"]
    if y_mode == "top":
        y = y_offset
    elif y_mode == "bottom":
        y = canvas_h - text_h - y_offset
    elif y_mode == "middle":
        y = (canvas_h - text_h) // 2
    else:
        y = y_offset

    draw.multiline_text(
        (x, y), text, font=font, fill=job["color_hex"], spacing=spacing,
        stroke_width=stroke_width, stroke_fill=job["stroke_color"], align="left",
    )
    img.save(job["out_path"])


def main():
    if len(sys.argv) != 2:
        print("사용법: render_caption.py <매니페스트.json>", file=sys.stderr)
        sys.exit(1)

    from PIL import Image, ImageDraw, ImageFont

    with open(sys.argv[1], "r", encoding="utf-8") as f:
        jobs = json.load(f)

    font_cache = {}
    errors = []
    for job in jobs:
        try:
            render_one(Image, ImageDraw, ImageFont, font_cache, job)
        except Exception as e:
            errors.append(f"{job.get('out_path', '?')}: {e}")

    if errors:
        print("\n".join(errors), file=sys.stderr)
        # 전부 실패했으면 exit 1(호출 쪽이 렌더링 실패로 처리), 일부만 실패했으면 exit 2(계속 진행 가능)
        sys.exit(1 if len(errors) == len(jobs) else 2)


if __name__ == "__main__":
    main()
