--Установка расшиения
create extension if not exists postgres_fdw;

--Создание триггерной функции
CREATE OR REPLACE FUNCTION public.tr_ins_upd_products()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
declare
	arr_images text[];
	arr_images_result text[];
	var text;
	flag int;
begin
	--В нижний регистр
	new.brand_name = LOWER(new.brand_name);
	new.category = LOWER(new.category);
	new.propertie_color = LOWER(new.propertie_color);
	new.propertie_material = LOWER(new.propertie_material);
	new.propertie_style = LOWER(new.propertie_style);
	new.shop_category_path = LOWER(new.shop_category_path);
	new.shop_name = LOWER(new.shop_name);
	new.category_path = LOWER(new.category_path);
	new.tags = LOWER(new.tags);

	new.unical_id = left(new.unical_id, 255);
	new.advertiser = left(new.advertiser, 255);
	--new.tags=left(new.tags, 255);
	--new.partner_link=left(new.partner_link, 255);
	new.ean = left(new.ean, 100);
	--new.alias=left(new.alias, 255);
	--new.title = left(new.title, 255);
	--new.images=left(new.images, 255);
	new.seo_title = left(new.seo_title, 255);
	--new.description=left(new.description, 255);
	--new.smart_description=left(new.smart_description, 255);
	new.seo_description = left(new.seo_description, 255);
	new.brand_name = left(new.brand_name, 255);
	new.brand_id = left(new.brand_id, 255);
	new.price = left(new.price, 255);
	new.price_old = left(new.price_old, 255);
	new.category = left(new.category, 255);
	new.category_id = left(new.category_id, 255);
	new.category_path = left(new.category_path, 255);
	new.shop_name = left(new.shop_name, 255);
	new.shop_id = left(new.shop_id, 255);
	new.shop_category_id = left(new.shop_category_id, 255);
	new.shop_category_path = left(new.shop_category_path, 255);
	new.delivery_time = left(new.delivery_time, 255);
	new.delivery_price = left(new.delivery_price, 255);
	new.propertie_style = left(new.propertie_style, 255);
	new.propertie_color = left(new.propertie_color, 255);
	new.propertie_material = left(new.propertie_material, 255);
	new.propertie_energy = left(new.propertie_energy, 255);
	new.propertie_size = left(new.propertie_size, 255);
	new.propertie_bank_sale = left(new.propertie_bank_sale, 255);
	new.propertie_rating = left(new.propertie_rating, 255);
	new.propertie_rating_sum = left(new.propertie_rating_sum, 255);
	new.propertie_height = left(new.propertie_height, 255);
	new.propertie_width = left(new.propertie_width, 255);
	new.propertie_length = left(new.propertie_length, 255);
	new.propertie_deep = left(new.propertie_deep, 255);

	--Символы <> в поле tags заменим запятой
	new.tags = regexp_replace(new.tags, '[<>]', ',','g');
	--Удалим амперсанд в поле tags
	new.tags = regexp_replace(new.tags, '[&]', '','g');
	--Удалим дубликаты в поле tags
	new.tags = array_to_string(array_remove(array(select distinct trim(unnest(string_to_array(new.tags, ',')))), ''), ',');
	--Удалим дубликаты в поле ean
	new.ean = array_to_string(array_remove(array(select distinct trim(unnest(string_to_array(new.ean, ',')))), ''), ',');

	--Удалим символы /-,пробел
	new.propertie_color = regexp_replace(new.propertie_color, '[-,// ]', '','g');
	new.ean = regexp_replace(new.ean, '[-,// ]', '','g');

	--Удалим noimage в поле images
	arr_images = array_remove(array(select distinct trim(unnest(string_to_array(new.images, ',')))), '');

	foreach var in array arr_images
	loop
		flag = 0;
		SELECT count(1) from regexp_matches(var, 'noimage', 'i') into flag;
		--Если нет совпадений то кидаем в результирующий массив
    	if (flag = 0) then
    		arr_images_result = array_append(arr_images_result, var);
    	end if;
  	end loop;

  	new.images = array_to_string(arr_images_result, ',');

	if ((new.images is null) or (new.images = '')) /*or ((new.ean is null) or (length(new.ean) < 13))*/ then
		RETURN null;
	else
		RETURN new;
	end if;
END;
$function$
;

--Создаем генератор для id
CREATE SEQUENCE if not EXISTS "products_id_seq"
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;

--Создадим таблицу
CREATE TABLE if not exists public.products (
	id int8 DEFAULT nextval('public.products_id_seq'::regclass) NOT NULL,
	unical_id text NULL,
	advertiser text NULL,
	"views" int8 NULL,
	tags text NULL,
	partner_link text NULL,
	ean text NULL,
	alias text NULL,
	title text NULL,
	images text NULL,
	seo_title text NULL,
	description text NULL,
	smart_description text NULL,
	seo_description text NULL,
	brand_name text NULL,
	brand_id text NULL,
	price text NULL,
	price_old text NULL,
	category text NULL,
	category_id text NULL,
	category_path text NULL,
	shop_name text NULL,
	shop_id text NULL,
	shop_category_id text NULL,
	shop_category_path text NULL,
	delivery_time text NULL,
	delivery_price text NULL,
	propertie_style text NULL,
	propertie_color text NULL,
	propertie_material text NULL,
	deal_time_from timestamptz NULL,
	deal_time_to timestamptz NULL,
	propertie_energy text NULL,
	propertie_size text NULL,
	propertie_bank_sale text NULL,
	propertie_rating text NULL,
	propertie_rating_sum text NULL,
	propertie_height text NULL,
	propertie_width text NULL,
	propertie_length text NULL,
	propertie_deep text NULL,
	active bool NULL,
	CONSTRAINT products_pkey PRIMARY KEY (id),
	CONSTRAINT products_un UNIQUE (unical_id,advertiser)
);

--Триггер на вставку и обновление для таблицы products
create trigger tr_import before insert or update on public.products for each row execute procedure tr_ins_upd_products();