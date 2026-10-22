package se.afshin.yavari.kafka.ui.serde;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.Optional;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;

/** Pretty-prints XML. DTDs are rejected outright, so no entity (external or not) is ever resolved. */
public final class XmlPretty {

    private XmlPretty() {}

    public static Optional<String> pretty(String text) {
        if (text == null || !text.stripLeading().startsWith("<")) {
            return Optional.empty();
        }
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            dbf.setFeature("http://xml.org/sax/features/external-general-entities", false);
            dbf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            dbf.setXIncludeAware(false);
            dbf.setExpandEntityReferences(false);
            dbf.setNamespaceAware(true);
            DocumentBuilder db = dbf.newDocumentBuilder();
            db.setErrorHandler(null);
            Document doc = db.parse(new InputSource(new StringReader(text)));
            stripBlankText(doc.getDocumentElement());

            TransformerFactory tf = TransformerFactory.newInstance();
            tf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            tf.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            tf.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
            Transformer t = tf.newTransformer();
            t.setOutputProperty(OutputKeys.INDENT, "yes");
            t.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
            t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            StringWriter out = new StringWriter();
            t.transform(new DOMSource(doc), new StreamResult(out));
            return Optional.of(out.toString().strip());
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private static void stripBlankText(Node node) {
        Node child = node.getFirstChild();
        while (child != null) {
            Node next = child.getNextSibling();
            if (child.getNodeType() == Node.TEXT_NODE && child.getTextContent().isBlank()) {
                node.removeChild(child);
            } else {
                stripBlankText(child);
            }
            child = next;
        }
    }
}
