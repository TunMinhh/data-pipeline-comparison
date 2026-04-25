"""
AttentionLSTM — wellness anomaly detector.

Architecture: stacked LSTM -> self-attention over timesteps -> regression head.
Used as an autoencoder-style anomaly detector: train to reconstruct a 7-day
window of user features, then score anomalies by reconstruction error.
"""

from __future__ import annotations

import torch
from torch import nn


class AttentionLSTM(nn.Module):
    """
    Input  : (batch, seq_len, input_size)  — per-user sequence of daily features
    Output : (batch, seq_len, input_size)  — reconstructed sequence
             attn_weights (batch, seq_len, 1)
    """

    def __init__(
        self,
        input_size: int,
        hidden_size: int = 64,
        num_layers: int = 2,
        dropout: float = 0.2,
    ):
        super().__init__()
        self.hidden_size = hidden_size

        self.encoder = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        # Simple additive attention over encoder timesteps
        self.attn = nn.Linear(hidden_size, 1)

        # Decoder re-expands context into a seq_len reconstruction
        self.decoder = nn.LSTM(
            input_size=hidden_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        self.out = nn.Linear(hidden_size, input_size)

    def forward(self, x: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
        # Encode
        enc_out, _ = self.encoder(x)                              # (B, T, H)

        # Attention over encoder steps
        attn_logits = self.attn(enc_out)                          # (B, T, 1)
        attn_weights = torch.softmax(attn_logits, dim=1)          # (B, T, 1)
        context = (attn_weights * enc_out).sum(dim=1, keepdim=True)  # (B, 1, H)

        # Broadcast context to seq_len and decode
        seq_len = x.size(1)
        context_broadcast = context.expand(-1, seq_len, -1)       # (B, T, H)
        dec_out, _ = self.decoder(context_broadcast)              # (B, T, H)
        recon = self.out(dec_out)                                 # (B, T, input_size)

        return recon, attn_weights


def reconstruction_error(x: torch.Tensor, recon: torch.Tensor) -> torch.Tensor:
    """Per-sample MSE — shape (batch,)."""
    return ((x - recon) ** 2).mean(dim=(1, 2))
