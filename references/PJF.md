# Prefeitura de Juiz de Fora

## Receitas
### Prevista
URL: https://www.pjf.mg.gov.br/transparencia/receitas/mensal/previsao/index.php

O botão `XLS` chama a função abaixo, que gera a URL: `https://www.pjf.mg.gov.br/transparencia/receitas/mensal/previsao/arquivos/xls/25.xls`
```js
function previsao_receita_mensal_xls(url)
{ 
	var result="arquivos/xls/"+
		   (url.ANO.options[url.ANO.selectedIndex].value)+
		   ".xls"
	 window.open(result)
}
```