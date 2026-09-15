from .base import BaseReranker, register_reranker
import logging
import warnings
import numpy as np
from loguru import logger
@register_reranker("local")
class LocalReranker(BaseReranker):
    def get_similarity_score(self, s1: list[str], s2: list[str]) -> np.ndarray:
        from sentence_transformers import SentenceTransformer
        import torch
        if not self.config.executor.debug:
            from transformers.utils import logging as transformers_logging
            from huggingface_hub.utils import logging as hf_logging
    
            transformers_logging.set_verbosity_error()
            hf_logging.set_verbosity_error()
            logging.getLogger("sentence_transformers").setLevel(logging.ERROR)
            logging.getLogger("sentence_transformers.SentenceTransformer").setLevel(logging.ERROR)
            logging.getLogger("transformers").setLevel(logging.ERROR)
            logging.getLogger("huggingface_hub").setLevel(logging.ERROR)
            logging.getLogger("huggingface_hub.utils._http").setLevel(logging.ERROR)
            warnings.filterwarnings("ignore", category=FutureWarning)

        requested_device = self.config.reranker.local.get("device", "auto")
        device = ("cuda" if torch.cuda.is_available() else "cpu") if requested_device == "auto" else requested_device
        logger.info(f"Embedding device: {device}; torch={torch.__version__}; CUDA={torch.version.cuda}")
        encoder = SentenceTransformer(self.config.reranker.local.model, trust_remote_code=True, device=device)
        if self.config.reranker.local.encode_kwargs:
            encode_kwargs = dict(self.config.reranker.local.encode_kwargs)
        else:
            encode_kwargs = {}
        try:
            features = encoder.encode(s1 + s2, **encode_kwargs, show_progress_bar=True)
        except torch.cuda.OutOfMemoryError:
            if requested_device != "auto" or device != "cuda":
                raise
            logger.warning("GPU memory exhausted; retrying embeddings on CPU")
            encoder.to("cpu")
            torch.cuda.empty_cache()
            features = encoder.encode(s1 + s2, **encode_kwargs, show_progress_bar=True)
        s1_feature, s2_feature = features[:len(s1)], features[len(s1):]
        sim = encoder.similarity(s1_feature, s2_feature)
        return sim.cpu().numpy()
