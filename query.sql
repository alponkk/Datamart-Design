-- Query untuk menghitung jumlah stok.
select
	sum(fs2.on_hand_stock) as "Jumlah Stok"
from
	data_mart_design.fact_stocks fs2
	--join data_mart_design.dim_entities de on
	--	de.entity_id = fs2.entity_id
	--join data_mart_design.dim_activities da on
	--	da.activity_id = fs2.activity_id
	--join data_mart_design.dim_materials dm on
	--	dm.material_id = fs2.material_id
	--where
	--	de.province = 'PROV. BANTEN'
	--	and da.activity_name in ('Rutin')
	--	and 
	--	(dm.vaccine_status > 0
	--		or dm.stockcount_status > 0
	--		or dm.addremove_status > 0
	--		or dm.openvial_status > 0)
;

-- Query untuk menampilkan stok per tag entitas.
select
	de.entity_tag as "Tag Entitas",
	sum(fs2.on_hand_stock) as "Jumlah Stok"
from
	data_mart_design.fact_stocks fs2
join data_mart_design.dim_entities de on
	de.entity_id = fs2.entity_id
	--join data_mart_design.dim_activities da on
	--	da.activity_id = fs2.activity_id
	--join data_mart_design.dim_materials dm on
	--	dm.material_id = fs2.material_id
	--where
	--	de.province = 'PROV. BANTEN'
	--	and da.activity_name in ('Rutin')
	--	and 
	--	(dm.vaccine_status > 0
	--		or dm.stockcount_status > 0
	--		or dm.addremove_status > 0
	--		or dm.openvial_status > 0)
group by
	"Tag Entitas"
order by
	"Jumlah Stok" desc
;


-- Query untuk menampilkan stok per material.
select
	dm.material_name as "Material",
	sum(fs2.on_hand_stock) as "Jumlah Stok"
from
	data_mart_design.fact_stocks fs2
	--join data_mart_design.dim_entities de on
	--	de.entity_id = fs2.entity_id
	--join data_mart_design.dim_activities da on
	--	da.activity_id = fs2.activity_id
join data_mart_design.dim_materials dm on
	dm.material_id = fs2.material_id
	--where
	--	de.province = 'PROV. BANTEN'
	--	and da.activity_name in ('Rutin')
	--	and 
	--	(dm.vaccine_status > 0
	--		or dm.stockcount_status > 0
	--		or dm.addremove_status > 0
	--		or dm.openvial_status > 0)
group by
	"Material"
order by
	"Jumlah Stok" desc