-- Cantidad de plublicaciones de seller que tienen multiples publicaciones
 select count(item_id) as cant_pub, seller_id from meli.best_seller_tbl
 group by seller_id
 having count(item_id) > 1
 order by 1 desc;
 
 -- promedio de ventas por seller 
 select round(sum(a.cant_vta_seller)/count(a.seller_id),2) as avg_sales_by_seller from (
 select count(1) as cant_vta_seller, seller_id from meli.best_seller_tbl
 where sales_qty != 0
 group by seller_id)a;
 
 -- Precio promedio en dolares
 select round(sum(a.precio_dolares)/count(distinct a.item_id)) as avg_price_usd
 from (
 select round(price*ratio,2) as precio_dolares, item_id, seller_id 
 from meli.best_seller_tbl) a;
 
 -- Porcentaje de artículos con garantía
 select round((b.art_c_garantia/count(distinct a.item_id))*100,0) as porc_item_warranty
 from meli.best_seller_tbl a,
 (
 select count(distinct item_id) as art_c_garantia from meli.best_seller_tbl
 where warranty != 'None') b;
 
 -- Metodos de shipping que ofrecen
 select distinct shipping as shipping_method from meli.best_seller_tbl;