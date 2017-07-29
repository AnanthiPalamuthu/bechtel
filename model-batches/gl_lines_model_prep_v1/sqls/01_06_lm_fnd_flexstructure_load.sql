select
    fifstl.application_id flex_structure_application_id ,
    fifstl.id_flex_code flex_structure_id_flex_code ,
    fifstl.id_flex_num flex_structure_id_flex_num ,
    fifstl.language flex_structure_language ,
    fifstl.id_flex_structure_name flex_structure_id_flex_structure_name ,
    fifstl.description flex_structure_description 
from
    ${source_hive_schema_name}.applsys_fnd_id_flex_structures_tl fifstl
