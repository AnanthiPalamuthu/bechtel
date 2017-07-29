select
   glh.*,
   glb.* 
from
    ${gl_header_cache} glh,
    ${gl_batch_cache} glb
where
    glh.head_je_header_id = glb.batch_je_batch_id
