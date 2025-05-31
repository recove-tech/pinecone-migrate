from typing import List
import numpy as np


def normalize_vector(vector: List[float]) -> List[float]:
    """
    Normalize a vector using L2 norm (Euclidean norm).

    Args:
        vector: List of float values representing the vector

    Returns:
        Normalized vector as a list of floats
    """
    vector_array = np.array(vector)
    norm = np.linalg.norm(vector_array)

    if norm == 0:
        return vector

    return (vector_array / norm).tolist()
