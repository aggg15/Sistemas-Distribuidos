"""Validaciones de argumentos compartidas por los programas de comunicaciones."""

import argparse
import math


def puerto(texto):
    try:
        valor = int(texto)
    except (TypeError, ValueError) as error:
        raise argparse.ArgumentTypeError("El puerto debe ser un entero") from error
    if not 1 <= valor <= 65535:
        raise argparse.ArgumentTypeError("El puerto debe estar entre 1 y 65535")
    return valor


def positivo(texto):
    try:
        valor = float(texto)
    except (TypeError, ValueError) as error:
        raise argparse.ArgumentTypeError("El valor debe ser numérico") from error
    if not math.isfinite(valor) or valor <= 0:
        raise argparse.ArgumentTypeError("El valor debe ser positivo y finito")
    return valor


def entero_positivo(texto):
    try:
        valor = int(texto)
    except (TypeError, ValueError) as error:
        raise argparse.ArgumentTypeError("El valor debe ser un entero") from error
    if valor <= 0:
        raise argparse.ArgumentTypeError("El valor debe ser mayor que cero")
    return valor


def campo_valido(texto):
    # Se reservan # y los caracteres de control; los espacios interiores y
    # los acentos sí son válidos. Se evita normalizar IDs silenciosamente.
    return bool(texto) and texto == texto.strip() and texto.isprintable() and "#" not in texto
