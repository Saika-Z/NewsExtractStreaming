/**
 * HtmlContentExtractor.java
 * Copyright (c) 2017, 海牛版权所有.
 * @author   qingniu
*/

package com.hainiu.spark.utils.extractor;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.*;

/**
 * @author   qingniu
 * @Date	 2017年7月15日 	 
 */
public class HtmlContentExtractor {
	
	private static final int P = 50;
	private static final double T = 0.65d;

	private static String[] excludeTags = {"a","link","script","input","form","noscript","style","iframe","ul"};
	
	private static String[] negativeWords = {"责任编辑","责编","本文来源","编辑：","编辑:","来源：","本报记者"};
	
	public static String CONTENT = "正文";

	private static boolean exclude(String tag){
		for(String e : excludeTags){
			if(e.equals(tag)){
				return true;
			}
		}
		return false;
	}
	/**
	 * 输入正文页html，提取正文的正规则xpath和多个反规则的xpath
	 * @param html 正文页html
	 * @return Map(正规则xpath=正文,反规则xpath1=编辑, 反规则xpath2=责编 ...)
	 */
	public static Map<String,String> generateXpath(String html){
		//Document dom = Jsoup.connect(url).userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0").get();
		Document dom = Jsoup.parse(html);
		Map<String,String> map = generateXpath(dom);
		return map;
	}

	/**
	 * 输入正文页url，提取正文的正规则xpath和多个反规则的xpath
	 * @param url 正文页url
	 * @return Map(正规则xpath=正文,反规则xpath1=编辑, 反规则xpath2=责编 ...)
			*/
	public static Map<String,String> generateXpath2(String url){
		Document dom;
		Map<String,String> xpath = null;
		try {
			dom = Jsoup.connect(url).userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0").get();
			xpath = generateXpath(dom);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return xpath;
	}
	
	/**
	 * 	该递归算法是找子节点中是否有P标签
	 *  1. 如果有P标签，判断P标签所占字数 / 节点总字数  是否 < T，如果小于 则认为是非法的P标签 ，无视掉
	 *  2. 如果没有P标签则选出子节点中文字比例 >= T 的继续递归查找，直到没有子节点为止
	 *     如果子节点中没有中文字比例 >= T 的，直接返回自身
	 *     
	 *  问题：如果一直没有P，则可能会返回一个错误的节点
	 */
	private static Element getDeepElementWithPTag(Element parent){
		Elements elements = parent.children();
		if(elements.size() == 0){
			return parent;
		}
		
		int pCount = 0;
		int pTextCount = 0;
		for(int i=0;i<elements.size();i++){
			Element e = elements.get(i);
			if(e.tagName().equals("p")){
				pCount ++;
				pTextCount += e.text().trim().length();
			}
		}
		if(pTextCount > 0){
			double rate = pTextCount * 1d / parent.text().trim().length();
			if(rate < T){
				pCount = 0;
			}
		}
		
		if(pCount == 0){
			for(int i=0;i<elements.size();i++){
				Element e = elements.get(i);
				double rate = e.text().trim().length() * 1d / parent.text().trim().length();
				//System.out.println(e.text().trim());
				if(rate >= T){
					return getDeepElementWithPTag(e);
				}
			}
		}
		return parent;
	}

	/**
	 * 提取节点对应的xpath
	 * @param e
	 * @return
	 */
	public static String getXPath(Element e){
		StringBuilder sb = new StringBuilder();
		while(e.parent()!=null){
			if(!"".equals(e.attr("id"))){
				if(e.tagName().equals("html") || e.tagName().equals("body")){
					sb.insert(0, e.tagName());
				}else{
					sb.insert(0, e.tagName() + "[@id='"+e.attr("id")+"']");
				}
				sb.insert(0, "//");
				break;
			}else if(!"".equals(e.attr("class"))) {
				if(e.tagName().equals("html") || e.tagName().equals("body")){
					sb.insert(0, e.tagName());
				}else{
					sb.insert(0, e.tagName() + "[@class='"+e.attr("class")+"']");
				}
				sb.insert(0, "/");
			}else{
				sb.insert(0, e.tagName());
				sb.insert(0, "/");
			}
			e = e.parent();
		}
		while(!sb.toString().startsWith("//")){
			sb.insert(0, "/");
		}
		return sb.toString();
	}

	/**
	 * 输入正文页document，提取正文的正规则xpath和多个反规则的xpath
	 * @param dom
	 * @return Map(正规则xpath=正文,反规则xpath1=编辑, 反规则xpath2=责编 ...)
	 */
	private static Map<String,String> generateXpath(Document dom){
		Map<String,String> resultMap = new HashMap<String,String>();
		try {
			//Document dom = Jsoup.connect(url).userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0").get();
			Element body = dom.body();
			Elements elements = body.getAllElements();
			// 干掉 {"a","link","script","input","form","noscript","style","iframe","ul"}
			for(int i=0;i<elements.size();i++){
				if(exclude(elements.get(i).tagName())){
					elements.get(i).remove();
				}
			}
			
			elements = body.children();
			List<SortedElement> candidacyElements = new ArrayList<SortedElement>();
			for(int i=0;i<elements.size();i++){
				Element e = elements.get(i);  //body下第一层 Element
				if(e.text().length() >= P){
					candidacyElements.add(new SortedElement(e,e.text().length()));
				}
			}
			
			if(candidacyElements.size() == 0){
				//一个大于50字的子节点都没有，就认为无正文
				return null;
			}
			
			Collections.sort(candidacyElements);
			
			Element contentElement = null;
			SortedElement se = candidacyElements.get(0);	//取得body下 文字最多的那个元素，遍历其子节点
			//System.out.println(se);
			Elements subChilds = se.getElement().children();
			for(int j=0;j<subChilds.size();j++){
				Element e = subChilds.get(j);
				String text = e.text();
				int len = text.length();
				double rate  = len * 1d / se.getTextLength();
				//System.out.println(rate);
				if(rate >= T) {
					contentElement = e;
					break;
				}
			}
			
			if(contentElement == null){
				//如果没有子节点、或者子节点中没有>=65%的，就把它自己返回
				contentElement = se.getElement();
			}
			
			//递归查找包含P的子节点，这是一个不断精确的过程
			Element finalElement = getDeepElementWithPTag(contentElement);
			Elements pElements = finalElement.children();
			int pCount = 0;
			for(int i=0;i<pElements.size();i++){
				Element e = pElements.get(i);
				if(e.tagName().equals("p")){
					pCount ++;
				}
			}
			
			if(pCount == 0){
				//递归之后，还是不包含P（通常是另类新闻模板或者陈旧的html写法）, 则使用之前找到的节点
				finalElement = contentElement;
			}
			//System.out.println(finalElement.tagName() +"\t" + finalElement.attributes());
			
			String xpath = getXPath(finalElement);
//			xpath = xpath + "/p";
//			JXDocument jx = new JXDocument(dom);
//			List sel = jx.sel(xpath);
//			Elements el = new Elements(sel);
//
//			System.out.println(el.text());
//			System.out.println(xpath);

			
			
			/*List<JXNode> list2 = jx.selN("//div[@id='main']/div[@class='nleft']/div[@class='content']/p[last()]");
			list2.get(0).getElement().remove();
			System.out.println("========================");
			System.out.println(list.get(0).getElement());*/


			try {
				resultMap = negativeXPath(finalElement, xpath);
			} catch (Exception e) {
//				e.printStackTrace();
			}
//            for (String xpa : resultMap.keySet()) {
//                System.out.println(xpa);
//                List sel1 = jx.sel(xpa);
//                Elements el1 = new Elements(sel1);
//                System.out.println(el1.text());
//            }
			if (resultMap == null)
				resultMap = new HashMap<String,String>();

			resultMap.put(xpath, CONTENT);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return resultMap;
	}

	/**
	 * 根据正文element和xpath，提取出反xpath集合
	 * @param element 正文element节点对象
	 * @param xpath 正文xpath
	 * @return Map(反规则xpath1=编辑, 反规则xpath2=责编 ...)
	 */
	private static Map<String,String> negativeXPath(Element element,String xpath){
		Elements elements = element.children();
		int len = element.text().length();
		Map<String,String> map = new HashMap<String,String>();
		for(int i=0;i<elements.size();i++){
			Element e = elements.get(i);
			for(String nw : negativeWords){
				if(e.text().indexOf(nw) != -1){
					int len1 = e.text().length();
					double rate  = len1 * 1d / len;
					//System.out.println(rate);
					if(rate > (1-T)) {
						//如果选出来的这个反规则占的字数 > 0.35 就不要了
						continue;
					}
					//System.out.println(e);
					// 当前p 节点如果有id属性，记录这个反规则

					String[] splits = xpath.split("/");
					String lastXpath = splits[splits.length - 1];
					String lastTagName = "";
					if(lastXpath.indexOf("[") != -1){
						lastTagName = lastXpath.substring(0, lastXpath.indexOf("["));
					}else{
						lastTagName = lastXpath;
					}
					System.out.println("lastTagName:" + lastTagName);

					// 不是p 节点
					if(!e.tagName().equalsIgnoreCase("p")){
						// p节点上游
						if(! e.parent().tagName().equalsIgnoreCase("p")){
							String fxpath = "";
							if(e.children().size() > 0){
								Elements children = e.children();
								String subTag = children.get(0).tagName();
								if(subTag.equalsIgnoreCase("p")){
									fxpath = xpath + "/" + e.tagName() + "/p[text()*=" + nw +"]";
								}else{
									fxpath = xpath + "/" + e.tagName() + "/" + subTag +"/span[text()*=" + nw +"]";
								}


							}else{
								fxpath = xpath + "/" + e.tagName() + "[text()*=" + nw +"]";
							}
							map.put(fxpath, nw);
						}else{
							// p节点下游
							String fxpath = xpath + "/" + e.tagName();
							map.put(fxpath, nw);
						}

					}else if(e.tagName().equalsIgnoreCase("p")){
						if(e.children().size() > 0){
							Elements subEls = e.children();
							boolean flag = false;
							for(Element sube : subEls){
								if(sube.text().indexOf(nw) != -1){
									String fxpath =  "";
									if(lastTagName.equalsIgnoreCase("p")){
										fxpath = xpath + "/" + sube.tagName();
									}else{
										fxpath = xpath + "/p/" + sube.tagName();
									}
									map.put(fxpath, nw);
									flag = true;
									break;
								}
							}
							if(!flag){
								String fxpath =  "";
								if(lastTagName.equalsIgnoreCase("p")){
									fxpath = xpath + "[text()*=" +  nw + "]";
								}else{
									fxpath = xpath + "/p[text()*=" +  nw + "]";
								}
								map.put(fxpath, nw);
							}

						}else{
							// 是p节点
							String fxpath = xpath + "/"+ e.tagName() + "[text()*=" + nw +"]";
							map.put(fxpath, nw);
						}

					}


//					}else if(e.nextElementSibling() == null){
//						//System.out.println("最后一个");
//						set.put(xpath+"/"+e.child(0).tagName(),nw);
//					}else if(e.previousElementSibling() == null){
//						//System.out.println("第一个");
//						set.put(xpath+"/"+e.tagName()+"[1]",nw);
//					}
				}
			}
		}
		//System.out.println(set);
		return map;
	}
	
	public static void main(String[] args) {
//		Map<String, String> stringStringMap = HtmlContentExtractor.generateXpath2("https://www.sohu.com/a/390639308_100034414");
		Map<String, String> map = HtmlContentExtractor.generateXpath2("http://www.bjnews.com.cn/inside/2017/07/15/450512.html");
		for(Map.Entry<String,String> entry : map.entrySet()){
			String xpath = entry.getKey();
			String value = entry.getValue();
			if(value.equals(HtmlContentExtractor.CONTENT)){
				System.out.println("正规则xpath：" + xpath);
			}else{
				System.out.println("反规则xpath：" + xpath);
			}
		}

	}
}

