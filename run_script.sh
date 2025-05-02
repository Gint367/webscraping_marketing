#!/bin/bash
# commented is done
# python webcrawl/crawl_domain.py -e merged_aluminiumwerke_20250408.csv > output_domain_content_aluminium.log
# python webcrawl/crawl_domain.py -e merged_anlagenbauer_20250408.csv > output_domain_content_anlagenbauer.log
# python webcrawl/crawl_domain.py -e merged_autozulieferer_20250408.csv > output_domain_content_autozulieferer.log
# python webcrawl/crawl_domain.py -e merged_blechteile_20250408.csv > output_domain_content_blechteile.log
# python webcrawl/crawl_domain.py -e merged_federn_20250408.csv > output_domain_content_federn.log
# python webcrawl/crawl_domain.py -e merged_fensterbauer_20250408.csv > output_domain_content_fensterbauer.log
# python webcrawl/crawl_domain.py -e merged_holzindustrie_20250408.csv > output_domain_content_holzindustrie.log
# python webcrawl/crawl_domain.py -e merged_komponenten_20250409.csv > output_domain_content_komponenten.log
# python webcrawl/crawl_domain.py -e merged_kunststoffteile_20250409.csv > output_domain_content_kunststoffteile.log
# python webcrawl/crawl_domain.py -e merged_maschinenbauer_20250408.csv > output_domain_content_maschinenbauer.log
# python webcrawl/crawl_domain.py -e merged_medizintechnik_20250409.csv > output_domain_content_medizintechnik.log
# python webcrawl/crawl_domain.py -e merged_mess-regeltechnik_20250409.csv > output_domain_content_mess-regeltechnik.log
# python webcrawl/crawl_domain.py -e merged_reifenhersteller_20250409.csv > output_domain_content_reifenhersteller.log
# python webcrawl/crawl_domain.py -e merged_stahlverarbeitung_20250408.csv > output_domain_content_technische-keramik.log
# python webcrawl/crawl_domain.py -e merged_stoffhersteller_20250409.csv > output_domain_content_technische-keramik.log
# python webcrawl/crawl_domain.py -e merged_technische-keramik_20250409.csv > output_domain_content_technische-keramik.log
# python webcrawl/crawl_domain.py -e merged_textilindustrie_20250409.csv > output_domain_content_textilindustrie.log
# python webcrawl/crawl_domain.py -e merged_werkzeugbau_20250408.csv > output_domain_content_werkzeugbau.log
# python webcrawl/crawl_domain.py -e merged_werkzeughersteller_20250408.csv > output_domain_content_werkzeughersteller.log
# python webcrawl/crawl_domain.py -e merged_zerspanungstechnik_20250409.csv > output_domain_content_zerspanungstechnik.log

# python webcrawl/extract_llm.py domain_content_aluminiumwerke --output llm_extracted_aluminiumwerke > output_llm_extracted_aluminiumwerke.log
# python webcrawl/extract_llm.py domain_content_anlagenbauer --output llm_extracted_anlagenbauer > output_llm_extracted_anlagenbauer.log
# python webcrawl/extract_llm.py domain_content_autozulieferer --output llm_extracted_autozulieferer > output_llm_extracted_autozulieferer.log
# python webcrawl/extract_llm.py domain_content_blechteile --output llm_extracted_blechteile > output_llm_extracted_blechteile.log
# python webcrawl/extract_llm.py domain_content_federn --output llm_extracted_federn > output_llm_extracted_federn.log
# python webcrawl/extract_llm.py domain_content_fensterbauer --output llm_extracted_fensterbauer > output_llm_extracted_fensterbauer.log
# python webcrawl/extract_llm.py domain_content_holzindustrie --output llm_extracted_holzindustrie > output_llm_extracted_holzindustrie.log
# python webcrawl/extract_llm.py domain_content_komponenten --output llm_extracted_komponenten > output_llm_extracted_komponenten.log
# python webcrawl/extract_llm.py domain_content_maschinenbauer --output llm_extracted_maschinenbauer > output_llm_extracted_maschinenbauer.log
# python webcrawl/extract_llm.py domain_content_medizintechnik --output llm_extracted_medizintechnik > output_llm_extracted_medizintechnik.log
# python webcrawl/extract_llm.py domain_content_mess-regeltechnik --output llm_extracted_mess-regeltechnik > output_llm_extracted_mess-regeltechnik.log
# python webcrawl/extract_llm.py domain_content_reifenhersteller --output llm_extracted_reifenhersteller > output_llm_extracted_reifenhersteller.log
# python webcrawl/extract_llm.py domain_content_stahlverarbeitung --output llm_extracted_stahlverarbeitung > output_llm_extracted_stahlverarbeitung.log
# python webcrawl/extract_llm.py domain_content_stoffhersteller --output llm_extracted_stoffhersteller > output_llm_extracted_stoffhersteller.log
# python webcrawl/extract_llm.py domain_content_technische-keramik --output llm_extracted_technische-keramik > output_llm_extracted_technische-keramik.log
# python webcrawl/extract_llm.py domain_content_textilindustrie --output llm_extracted_textilindustrie > output_llm_extracted_textilindustrie.log
# python webcrawl/extract_llm.py domain_content_werkzeugbau --output llm_extracted_werkzeugbau > output_llm_extracted_werkzeugbau.log
# python webcrawl/extract_llm.py domain_content_werkzeughersteller --output llm_extracted_werkzeughersteller > output_llm_extracted_werkzeughersteller.log
# python webcrawl/extract_llm.py domain_content_zerspanungstechnik --output llm_extracted_zerspanungstechnik > output_llm_extracted_zerspanungstechnik.log

# python webcrawl/fill_process_type.py --folder llm_extracted_autozulieferer --output-dir process_filled_autozulieferer > output_fill_process_anlagenbauer.log
# python webcrawl/fill_process_type.py --folder llm_extracted_blechteile --output-dir process_filled_blechteile > output_fill_process_blechteile.log
# python webcrawl/fill_process_type.py --folder llm_extracted_federn --output-dir process_filled_federn > output_fill_process_federn.log
# python webcrawl/fill_process_type.py --folder llm_extracted_fensterbauer --output-dir process_filled_fensterbauer > output_fill_process_fensterbauer.log
# python webcrawl/fill_process_type.py --folder llm_extracted_holzindustrie --output-dir process_filled_holzindustrie > output_fill_process_holzindustrie.log
# python webcrawl/fill_process_type.py --folder llm_extracted_komponenten --output-dir process_filled_komponenten > output_fill_process_komponenten.log
# python webcrawl/fill_process_type.py --folder llm_extracted_maschinenbauer --output-dir process_filled_maschinenbauer > output_fill_process_maschinenbauer.log
# python webcrawl/fill_process_type.py --folder llm_extracted_medizintechnik --output-dir process_filled_medizintechnik > output_fill_process_medizintechnik.log
# python webcrawl/fill_process_type.py --folder llm_extracted_mess-regeltechnik --output-dir process_filled_mess-regeltechnik > output_fill_process_mess-regeltechnik.log
# python webcrawl/fill_process_type.py --folder llm_extracted_reifenhersteller --output-dir process_filled_reifenhersteller > output_fill_process_reifenhersteller.log
# python webcrawl/fill_process_type.py --folder llm_extracted_stahlverarbeitung --output-dir process_filled_stahlverarbeitung > output_fill_process_stahlverarbeitung.log
# python webcrawl/fill_process_type.py --folder llm_extracted_stoffhersteller --output-dir process_filled_stoffhersteller > output_fill_process_stoffhersteller.log
# python webcrawl/fill_process_type.py --folder llm_extracted_technische-keramik --output-dir process_filled_technische-keramik > output_fill_process_technische-keramik.log
# python webcrawl/fill_process_type.py --folder llm_extracted_textilindustrie --output-dir process_filled_textilindustrie > output_fill_process_textilindustrie.log
# python webcrawl/fill_process_type.py --folder llm_extracted_werkzeugbau --output-dir process_filled_werkzeugbau > output_fill_process_werkzeugbau.log
# python webcrawl/fill_process_type.py --folder llm_extracted_werkzeughersteller --output-dir process_filled_werkzeughersteller > output_fill_process_werkzeughersteller.log
# python webcrawl/fill_process_type.py --folder llm_extracted_zerspanungstechnik --output-dir process_filled_zerspanungstechnik > output_fill_process_zerspanungstechnik.log

python webcrawl/pluralize_with_llm.py  --input process_filled_aluminiumwerke/ --output pluralized_aluminiumwerke > output_pluralized_aluminiumwerke.log
python webcrawl/pluralize_with_llm.py  --input process_filled_anlagenbauer/ --output pluralized_anlagenbauer > output_pluralized_anlagenbauer.log
python webcrawl/pluralize_with_llm.py  --input process_filled_autozulieferer/ --output pluralized_autozulieferer > output_pluralized_autozulieferer.log
python webcrawl/pluralize_with_llm.py  --input process_filled_blechteile/ --output pluralized_blechteile > output_pluralized_blechteile.log
python webcrawl/pluralize_with_llm.py  --input process_filled_federn/ --output pluralized_federn > output_pluralized_federn.log
python webcrawl/pluralize_with_llm.py  --input process_filled_fensterbauer/ --output pluralized_fensterbauer > output_pluralized_fensterbauer.log
python webcrawl/pluralize_with_llm.py  --input process_filled_holzindustrie/ --output pluralized_holzindustrie > output_pluralized_holzindustrie.log
python webcrawl/pluralize_with_llm.py  --input process_filled_komponenten/ --output pluralized_komponenten > output_pluralized_komponenten.log
python webcrawl/pluralize_with_llm.py  --input process_filled_maschinenbauer/ --output pluralized_maschinenbauer > output_pluralized_maschinenbauer.log
python webcrawl/pluralize_with_llm.py  --input process_filled_medizintechnik/ --output pluralized_medizintechnik > output_pluralized_medizintechnik.log
python webcrawl/pluralize_with_llm.py  --input process_filled_mess-regeltechnik/ --output pluralized_mess-regeltechnik > output_pluralized_mess-regeltechnik.log
python webcrawl/pluralize_with_llm.py  --input process_filled_reifenhersteller/ --output pluralized_reifenhersteller > output_pluralized_reifenhersteller.log
python webcrawl/pluralize_with_llm.py  --input process_filled_stahlverarbeitung/ --output pluralized_stahlverarbeitung > output_pluralized_stahlverarbeitung.log
python webcrawl/pluralize_with_llm.py  --input process_filled_stoffhersteller/ --output pluralized_stoffhersteller > output_pluralized_stoffhersteller.log
python webcrawl/pluralize_with_llm.py  --input process_filled_technische-keramik/ --output pluralized_technische-keramik > output_pluralized_technische-keramik.log
python webcrawl/pluralize_with_llm.py  --input process_filled_textilindustrie/ --output pluralized_textilindustrie > output_pluralized_textilindustrie.log
python webcrawl/pluralize_with_llm.py  --input process_filled_werkzeugbau/ --output pluralized_werkzeugbau > output_pluralized_werkzeugbau.log
python webcrawl/pluralize_with_llm.py  --input process_filled_werkzeughersteller/ --output pluralized_werkzeughersteller > output_pluralized_werkzeughersteller.log
python webcrawl/pluralize_with_llm.py  --input process_filled_zerspanungstechnik/ --output pluralized_zerspanungstechnik > output_pluralized_zerspanungstechnik.log 