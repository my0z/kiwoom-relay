#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CAPTION_FONT_PATHS에 있는 폰트들이 완성형 한글(가~힣, 11,172자)을 몇 % 갖고 있는지 검사.
사용법: python3 check_font_coverage.py
"""
from fontTools.ttLib import TTFont

FONTS = {
    "gowun": "/usr/local/share/fonts/GowunDodum-Regular.ttf",
    "nanumpen": "/usr/local/share/fonts/NanumPenScript-Regular.ttf",
    "gowunbatang": "/usr/local/share/fonts/GowunBatang-Regular.ttf",
    "songmyung": "/usr/local/share/fonts/SongMyung-Regular.ttf",
    "gaegu": "/usr/local/share/fonts/Gaegu-Regular.ttf",
    "himelody": "/usr/local/share/fonts/HiMelody-Regular.ttf",
    "poorstory": "/usr/local/share/fonts/PoorStory-Regular.ttf",
    "gamjaflower": "/usr/local/share/fonts/GamjaFlower-Regular.ttf",
    "singleday": "/usr/local/share/fonts/SingleDay-Regular.ttf",
    "cutefont": "/usr/local/share/fonts/CuteFont-Regular.ttf",
    "stylish": "/usr/local/share/fonts/Stylish-Regular.ttf",
    "yeonsung": "/usr/local/share/fonts/YeonSung-Regular.ttf",
    "gugi": "/usr/local/share/fonts/Gugi-Regular.ttf",
    "nanumbrush": "/usr/local/share/fonts/NanumBrushScript-Regular.ttf",
    "sunflower": "/usr/local/share/fonts/Sunflower-Light.ttf",
    "noto": "/usr/local/share/fonts/NotoSansKR-Regular.otf",
}

ALL_HANGUL = [chr(cp) for cp in range(0xAC00, 0xD7A4)]  # 완성형 한글 11,172자 전부

for key, path in FONTS.items():
    try:
        font = TTFont(path, fontNumber=0, lazy=True)
        cmap = font.getBestCmap()
        covered = sum(1 for ch in ALL_HANGUL if ord(ch) in cmap)
        pct = covered / len(ALL_HANGUL) * 100
        print(f"{key:15s} {covered:6d}/{len(ALL_HANGUL)} ({pct:5.1f}%)  {path}")
    except Exception as e:
        print(f"{key:15s} 오류: {e}  ({path})")
