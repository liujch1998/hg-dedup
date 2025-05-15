from cpp_engine_dedup import EngineDedup_U8

HACK = 1000
engine = EngineDedup_U8(["/data/jiachengl/hg-dedup/index/v6_pileval_u8"], False)
engine.verify_sa_correctness(HACK)
