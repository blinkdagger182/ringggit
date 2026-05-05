//
//  HomeAIAssistantOverlay.swift
//  dime
//

import SwiftUI
import UIKit

struct HomeAIAssistantOverlay: View {
    private let assistantTitle = "Renvo AI"

    @ObservedObject var viewModel: HomeAIAssistantViewModel
    @StateObject private var keyboardHeightHelper = KeyboardHeightHelper()

    let namespace: Namespace.ID
    let topInset: CGFloat
    let bottomInset: CGFloat
    let revealProgress: CGFloat
    let isExpanded: Bool
    let onCollapse: () -> Void
    let collapseGesture: AnyGesture<DragGesture.Value>?

    @State private var composerFocused = false
    @State private var composerFocusRequestID = 0
    @State private var chatScrollRequestID = 0
    @State private var showAttachmentSheet = false

    var body: some View {
        GeometryReader { proxy in
            let keyboardOverlap = max(0, keyboardHeightHelper.keyboardHeight - proxy.safeAreaInsets.bottom)
            let baseComposerBottomPadding = max(proxy.safeAreaInsets.bottom, bottomInset) + 12
            let activeComposerBottomPadding = keyboardOverlap > 0 ? 12 : baseComposerBottomPadding

            ZStack(alignment: .bottom) {
                Color(hex: "1a0533")
                    .ignoresSafeArea()

                HomeAIAnimatedGradientBackground()
                    .opacity(max(0, revealProgress))
                    .ignoresSafeArea()

VStack(spacing: 0) {
                    previewHeader
                        .padding(.horizontal, 24)
                        .padding(.top, max(topInset, proxy.safeAreaInsets.top) + 10)
                        .opacity(revealProgress)

                    messageArea(
                        keyboardOverlap: keyboardOverlap,
                        composerBottomPadding: activeComposerBottomPadding
                    )
                        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
                        .opacity(revealProgress)
                        .offset(y: (1 - revealProgress) * 24)
                }
                .frame(width: proxy.size.width, height: proxy.size.height, alignment: .top)

                composerSection
                    .padding(.bottom, activeComposerBottomPadding)
                    .opacity(revealProgress)
                    .offset(y: (1 - revealProgress) * 20 - keyboardOverlap)
                    .background(
                        LinearGradient(
                            colors: [
                                Color.black.opacity(0),
                                Color.black.opacity(0.18),
                                Color.black.opacity(0.34)
                            ],
                            startPoint: .top,
                            endPoint: .bottom
                        )
                    )
                    .zIndex(2)

                if isExpanded && keyboardOverlap == 0 {
                    collapseHandle
                        .padding(.bottom, activeComposerBottomPadding + 104)
                        .opacity(revealProgress)
                        .zIndex(1)
                        .modifier(HomeAICollapseGestureModifier(dragGesture: collapseGesture))
                }
            }
            .ignoresSafeArea(.keyboard, edges: .bottom)
            .onChange(of: isExpanded) { expanded in
                guard expanded else { return }
                guard viewModel.messages.isEmpty else { return }

                DispatchQueue.main.asyncAfter(deadline: .now() + 0.38) {
                    guard isExpanded else { return }
                    focusComposer()
                }
            }
        }
        .sheet(isPresented: $showAttachmentSheet) {
            let sheet = HomeAIAttachmentSheet(
                attachments: $viewModel.pendingAttachments,
                onQuickPrompt: { prompt in
                    viewModel.draftMessage = prompt
                    viewModel.sendDraftMessage()
                }
            )
            if #available(iOS 16.0, *) {
                sheet
                    .presentationDetents([.medium])
                    .presentationDragIndicator(.hidden)
            } else {
                sheet
            }
        }
    }

    private var previewHeader: some View {
        HStack(spacing: 10) {
            Image(systemName: "sparkles")
                .font(.system(size: 15, weight: .semibold))

            Text(assistantTitle)
                .font(.system(size: 21, weight: .semibold, design: .rounded))

            Text("beta")
                .font(.system(.subheadline, design: .rounded).weight(.semibold))
                .padding(.horizontal, 10)
                .padding(.vertical, 5)
                .overlay(
                    Capsule(style: .continuous)
                        .stroke(Color.white.opacity(0.24), lineWidth: 1)
                )
        }
        .foregroundColor(.white.opacity(0.88))
        .frame(maxWidth: .infinity)
    }

    private func messageArea(keyboardOverlap: CGFloat, composerBottomPadding: CGFloat) -> some View {
        ScrollViewReader { proxy in
            ScrollView(showsIndicators: false) {
                VStack(alignment: .leading, spacing: 16) {
                    if viewModel.messages.isEmpty {
                        emptyState
                    } else {
                        quickActionsSection(title: "Try asking")

                        ForEach(viewModel.messages) { message in
                            messageBubble(message)
                                .id(message.id)
                        }

                        if viewModel.isResponding {
                            typingIndicator
                                .id("typing-indicator")
                        }
                    }

                    nonBubbleDragArea(
                        height: bottomScrollSpacerHeight(
                            keyboardOverlap: keyboardOverlap,
                            composerBottomPadding: composerBottomPadding
                        )
                    )
                    .id("bottom-anchor")
                }
                .background(
                    Color.black.opacity(0.001)
                        .contentShape(Rectangle())
                        .onTapGesture {
                            dismissKeyboard()
                        }
                )
                .padding(.horizontal, 24)
                .padding(.top, 16)
            }
            .modifier(HomeAIInteractiveKeyboardDismissModifier())
            .background(
                nonBubbleDragArea(height: 1)
            )
            .onChange(of: viewModel.messages.count) { _ in
                requestScrollToChatEnd(proxy, animated: false)
            }
            .onChange(of: viewModel.isResponding) { _ in
                requestScrollToChatEnd(proxy, animated: false)
            }
            .onChange(of: keyboardHeightHelper.keyboardHeight) { _ in
                requestScrollToChatEnd(proxy, animated: false, delay: 0.26)
            }
            .onChange(of: composerFocused) { focused in
                guard focused else { return }
                requestScrollToChatEnd(proxy, animated: false, delay: 0.26)
            }
            .onAppear {
                guard !viewModel.messages.isEmpty else { return }
                requestScrollToChatEnd(proxy, animated: false, delay: 0)
            }
        }
    }

    private func nonBubbleDragArea(height: CGFloat) -> some View {
        Color.black.opacity(0.001)
            .frame(maxWidth: .infinity, minHeight: height, maxHeight: height)
            .contentShape(Rectangle())
            .onTapGesture {
                dismissKeyboard()
            }
            .modifier(HomeAICollapseGestureModifier(dragGesture: collapseGesture))
    }

    private var emptyState: some View {
        VStack(spacing: 18) {
            Text("How can I help you?")
                .font(.system(size: 30, weight: .medium, design: .serif))
                .tracking(-0.4)
                .foregroundColor(.white.opacity(0.96))
                .multilineTextAlignment(.center)
                .padding(.top, 20)

            quickActionsSection(title: "")

            Text("Try: \"How much did I spend on food this week?\"")
                .font(.system(.subheadline, design: .rounded).weight(.medium))
                .foregroundColor(.white.opacity(0.42))
        }
    }

    private func quickActionsSection(title: String) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            if !title.isEmpty {
                Text(title)
                    .font(.system(.footnote, design: .rounded).weight(.medium))
                    .foregroundColor(.white.opacity(0.42))
            }

            ScrollView(.horizontal, showsIndicators: false) {
                HStack(spacing: 12) {
                    ForEach(viewModel.quickActions) { quickAction in
                        Button {
                            viewModel.triggerQuickAction(quickAction)
                        } label: {
                            VStack(alignment: .leading, spacing: 10) {
                                Image(systemName: quickAction.systemImage)
                                    .font(.system(size: 14, weight: .semibold))
                                    .foregroundColor(.white.opacity(0.72))

                                Spacer(minLength: 0)

                                Text(quickAction.title)
                                    .font(.system(.headline, design: .rounded).weight(.medium))
                                    .foregroundColor(.white.opacity(0.92))
                                    .lineLimit(2)
                                    .multilineTextAlignment(.leading)

                                Text(quickActionSubtitle(for: quickAction.title))
                                    .font(.system(.footnote, design: .rounded))
                                    .foregroundColor(.white.opacity(0.46))
                                    .lineLimit(2)
                            }
                            .padding(16)
                            .frame(width: 146, height: 146, alignment: .leading)
                            .background(
                                RoundedRectangle(cornerRadius: 16, style: .continuous)
                                    .fill(Color(hex: "141414").opacity(0.96))
                                    .overlay(
                                        RoundedRectangle(cornerRadius: 16, style: .continuous)
                                            .stroke(Color(hex: "222222"), lineWidth: 1)
                                    )
                            )
                        }
                        .buttonStyle(.plain)
                    }
                }
                .padding(.trailing, 24)
            }
            .scrollClipDisabledIfAvailable()
        }
    }

    private func messageBubble(_ message: HomeAIMessage) -> some View {
        HStack {
            if message.role == .assistant {
                VStack(alignment: .leading, spacing: 6) {
                    Text("Renvo")
                        .font(.system(.caption, design: .rounded).weight(.medium))
                        .foregroundColor(.white.opacity(0.38))

                    Text(message.text)
                        .font(.system(.body, design: .rounded))
                        .foregroundColor(.white.opacity(0.92))
                        .multilineTextAlignment(.leading)
                }
                .padding(16)
                .background(
                    RoundedRectangle(cornerRadius: 16, style: .continuous)
                        .fill(Color(hex: "141414").opacity(0.96))
                        .overlay(
                            RoundedRectangle(cornerRadius: 16, style: .continuous)
                                .stroke(Color(hex: "222222"), lineWidth: 1)
                        )
                )

                Spacer(minLength: 42)
            } else {
                Spacer(minLength: 42)

                VStack(alignment: .trailing, spacing: 6) {
                    if !message.attachmentThumbnails.isEmpty {
                        ScrollView(.horizontal, showsIndicators: false) {
                            HStack(spacing: 6) {
                                ForEach(message.attachmentThumbnails.indices, id: \.self) { i in
                                    Image(uiImage: message.attachmentThumbnails[i])
                                        .resizable()
                                        .scaledToFill()
                                        .frame(width: 120, height: 90)
                                        .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))
                                }
                            }
                        }
                        .frame(maxWidth: 260)
                    }

                    if !message.text.isEmpty && message.text != "Please extract all transactions from the attached image(s)." {
                        Text(message.text)
                            .font(.system(.body, design: .rounded).weight(.medium))
                            .foregroundColor(.white.opacity(0.94))
                            .padding(.horizontal, 16)
                            .padding(.vertical, 13)
                            .background(
                                RoundedRectangle(cornerRadius: 16, style: .continuous)
                                    .fill(Color(hex: "1A1A1A"))
                            )
                    }
                }
            }
        }
    }

    private var typingIndicator: some View {
        HStack {
            HStack(spacing: 6) {
                Circle()
                    .fill(Color.white.opacity(0.8))
                    .frame(width: 7, height: 7)
                Circle()
                    .fill(Color.white.opacity(0.6))
                    .frame(width: 7, height: 7)
                Circle()
                    .fill(Color.white.opacity(0.4))
                    .frame(width: 7, height: 7)
            }
            .padding(.horizontal, 16)
            .padding(.vertical, 14)
            .background(
                RoundedRectangle(cornerRadius: 16, style: .continuous)
                    .fill(Color(hex: "141414").opacity(0.96))
                    .overlay(
                        RoundedRectangle(cornerRadius: 16, style: .continuous)
                            .stroke(Color(hex: "222222"), lineWidth: 1)
                    )
            )

            Spacer(minLength: 42)
        }
    }

    private var composerSection: some View {
        VStack(spacing: 8) {
            if !viewModel.pendingAttachments.isEmpty {
                attachmentStrip
                    .padding(.horizontal, 20)
            }

            HStack(alignment: .center, spacing: 8) {
                if keyboardHeightHelper.keyboardHeight > 0 {
                    Button { showAttachmentSheet = true } label: {
                        ZStack {
                            Circle()
                                .fill(Color(hex: "1A1A1A"))
                                .frame(width: 50, height: 50)
                            Image(systemName: "plus")
                                .font(.system(size: 20, weight: .medium))
                                .foregroundColor(.white.opacity(0.65))
                        }
                    }
                    .buttonStyle(.plain)
                }

                composer
            }
            .padding(.horizontal, 20)
        }
    }

    private var attachmentStrip: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: 8) {
                ForEach(viewModel.pendingAttachments) { item in
                    ZStack(alignment: .topTrailing) {
                        ZStack(alignment: .bottomLeading) {
                            Image(uiImage: item.thumbnail)
                                .resizable()
                                .scaledToFill()
                                .frame(width: 64, height: 64)
                                .clipShape(RoundedRectangle(cornerRadius: 10, style: .continuous))

                            if item.pages.count > 1 {
                                Text(item.label.components(separatedBy: "·").last?.trimmingCharacters(in: .whitespaces) ?? "")
                                    .font(.system(size: 9, weight: .semibold, design: .rounded))
                                    .foregroundColor(.white)
                                    .padding(.horizontal, 5)
                                    .padding(.vertical, 2)
                                    .background(Color.black.opacity(0.55))
                                    .clipShape(Capsule())
                                    .padding(4)
                            }
                        }

                        Button {
                            viewModel.removeAttachment(id: item.id)
                        } label: {
                            ZStack {
                                Circle()
                                    .fill(Color.black.opacity(0.65))
                                    .frame(width: 20, height: 20)
                                Image(systemName: "xmark")
                                    .font(.system(size: 9, weight: .bold))
                                    .foregroundColor(.white)
                            }
                            .padding(3)
                        }
                        .buttonStyle(.plain)
                    }
                }
            }
        }
        .frame(height: 70)
    }

    private var composer: some View {
        HStack(alignment: .center, spacing: 12) {
            if keyboardHeightHelper.keyboardHeight == 0 {
                Button { showAttachmentSheet = true } label: {
                    Image(systemName: "plus")
                        .font(.system(size: 20, weight: .medium))
                        .foregroundColor(.white.opacity(0.6))
                        .frame(width: 28, height: 28)
                }
                .buttonStyle(.plain)
            }

            HomeAIComposerTextField(
                text: $viewModel.draftMessage,
                isFocused: composerFocusBinding,
                focusRequestID: composerFocusRequestID,
                placeholder: "Ask anything about your money...",
                onSubmit: sendComposerMessage
            )
            .frame(maxWidth: .infinity, minHeight: 24, maxHeight: 24, alignment: .leading)

            if viewModel.hasContent {
                Button { sendComposerMessage() } label: {
                    Image(systemName: "arrow.up.circle.fill")
                        .font(.system(size: 30))
                        .foregroundColor(.white)
                }
                .buttonStyle(.plain)
            }
        }
        .frame(height: 56)
        .padding(.horizontal, 18)
        .contentShape(Rectangle())
        .onTapGesture {
            focusComposer()
        }
        .background(
            RoundedRectangle(cornerRadius: 20, style: .continuous)
                .fill(Color(hex: "1A1A1A"))
                .overlay(
                    RoundedRectangle(cornerRadius: 20, style: .continuous)
                        .stroke(composerFocused ? Color.white.opacity(0.16) : Color.clear, lineWidth: 1)
                )
                .shadow(color: composerFocused ? Color.white.opacity(0.08) : Color.black.opacity(0.12), radius: composerFocused ? 18 : 12, y: 8)
        )
        .simultaneousGesture(
            DragGesture(minimumDistance: 4)
                .onChanged { value in
                    guard value.translation.height > 8 else { return }
                    dismissKeyboard()
                }
        )
    }

    private var collapseHandle: some View {
        ZStack {
            Image(systemName: "chevron.up")
                .font(.system(size: 20, weight: .semibold))
                .foregroundColor(.white.opacity(0.46))
                .frame(width: 48, height: 28)
        }
        .frame(width: 112, height: 64)
        .contentShape(Rectangle())
        .accessibilityLabel("Swipe up to return Home")
    }

    private func requestScrollToChatEnd(_ proxy: ScrollViewProxy, animated: Bool, delay: TimeInterval = 0.04) {
        chatScrollRequestID += 1
        let requestID = chatScrollRequestID

        DispatchQueue.main.asyncAfter(deadline: .now() + delay) {
            guard requestID == chatScrollRequestID else { return }

            let action = {
                proxy.scrollTo("bottom-anchor", anchor: .bottom)
            }

            if animated {
                withAnimation(.easeOut(duration: 0.2)) {
                    action()
                }
            } else {
                action()
            }
        }
    }

    private func sendComposerMessage() {
        guard !viewModel.draftMessage.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return }
        guard !viewModel.isResponding else { return }

        focusComposer()
        viewModel.sendDraftMessage()
        focusComposer()

        DispatchQueue.main.async {
            focusComposer()
        }

        DispatchQueue.main.asyncAfter(deadline: .now() + 0.05) {
            focusComposer()
        }

        DispatchQueue.main.asyncAfter(deadline: .now() + 0.18) {
            focusComposer()
        }
    }

    private func dismissKeyboard() {
        composerFocused = false
        UIApplication.shared.endEditing()
    }

    private func bottomScrollSpacerHeight(keyboardOverlap: CGFloat, composerBottomPadding: CGFloat) -> CGFloat {
        72 + composerBottomPadding + keyboardOverlap
    }

    private func focusComposer() {
        composerFocused = true
        composerFocusRequestID += 1
    }

    private func quickActionSubtitle(for title: String) -> String {
        switch title {
        case "Add transaction":
            return "Quickly log an expense"
        case "Where did my money go?":
            return "Breakdown by category"
        case "Show this month's spending":
            return "Review this month's flow"
        case "Can I afford this?":
            return "Check against current budget"
        default:
            return "Start with a suggested prompt"
        }
    }

    private var composerFocusBinding: Binding<Bool> {
        Binding(
            get: { composerFocused },
            set: { composerFocused = $0 }
        )
    }
}

private struct HomeAICollapseGestureModifier: ViewModifier {
    let dragGesture: AnyGesture<DragGesture.Value>?

    func body(content: Content) -> some View {
        if let dragGesture {
            content.highPriorityGesture(dragGesture)
        } else {
            content
        }
    }
}

private struct HomeAIInteractiveKeyboardDismissModifier: ViewModifier {
    func body(content: Content) -> some View {
        if #available(iOS 16.0, *) {
            content.scrollDismissesKeyboard(.interactively)
        } else {
            content
        }
    }
}

private extension View {
    @ViewBuilder
    func scrollClipDisabledIfAvailable() -> some View {
        if #available(iOS 17.0, *) {
            self.scrollClipDisabled()
        } else {
            self
        }
    }
}

private struct HomeAIComposerTextField: UIViewRepresentable {
    @Binding var text: String
    @Binding var isFocused: Bool

    let focusRequestID: Int
    let placeholder: String
    let onSubmit: () -> Void

    func makeUIView(context: Context) -> UITextField {
        let textField = UITextField(frame: .zero)
        textField.delegate = context.coordinator
        textField.backgroundColor = .clear
        textField.borderStyle = .none
        textField.returnKeyType = .send
        textField.textColor = UIColor.white.withAlphaComponent(0.95)
        textField.tintColor = UIColor.white
        textField.font = UIFont.roundedSystemFont(ofSize: 17, weight: .regular)
        textField.autocorrectionType = .yes
        textField.autocapitalizationType = .sentences
        textField.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        textField.addTarget(context.coordinator, action: #selector(Coordinator.textDidChange(_:)), for: .editingChanged)
        updatePlaceholder(for: textField)
        return textField
    }

    func updateUIView(_ textField: UITextField, context: Context) {
        context.coordinator.parent = self

        if textField.text != text {
            textField.text = text
        }

        updatePlaceholder(for: textField)

        if context.coordinator.lastFocusRequestID != focusRequestID {
            context.coordinator.lastFocusRequestID = focusRequestID
            DispatchQueue.main.async {
                textField.becomeFirstResponder()
            }
        } else if isFocused && !textField.isFirstResponder {
            DispatchQueue.main.async {
                textField.becomeFirstResponder()
            }
        } else if !isFocused && textField.isFirstResponder {
            textField.resignFirstResponder()
        }
    }

    func makeCoordinator() -> Coordinator {
        Coordinator(parent: self)
    }

    private func updatePlaceholder(for textField: UITextField) {
        textField.attributedPlaceholder = NSAttributedString(
            string: placeholder,
            attributes: [
                .foregroundColor: UIColor.white.withAlphaComponent(0.34),
                .font: UIFont.roundedSystemFont(ofSize: 17, weight: .regular)
            ]
        )
    }

    final class Coordinator: NSObject, UITextFieldDelegate {
        var parent: HomeAIComposerTextField
        var lastFocusRequestID: Int

        init(parent: HomeAIComposerTextField) {
            self.parent = parent
            self.lastFocusRequestID = parent.focusRequestID
        }

        @objc func textDidChange(_ textField: UITextField) {
            parent.text = textField.text ?? ""
        }

        func textFieldDidBeginEditing(_ textField: UITextField) {
            parent.isFocused = true
        }

        func textFieldDidEndEditing(_ textField: UITextField) {
            parent.isFocused = false
        }

        func textFieldShouldReturn(_ textField: UITextField) -> Bool {
            parent.onSubmit()
            return false
        }
    }
}

private extension UIFont {
    static func roundedSystemFont(ofSize size: CGFloat, weight: UIFont.Weight) -> UIFont {
        let systemFont = UIFont.systemFont(ofSize: size, weight: weight)
        guard let descriptor = systemFont.fontDescriptor.withDesign(.rounded) else {
            return systemFont
        }
        return UIFont(descriptor: descriptor, size: size)
    }
}

private struct HomeAIAnimatedGradientBackground: View {
    var body: some View {
        if #available(iOS 18.0, *) {
            TimelineView(.animation) { timeline in
                let t = Float(timeline.date.timeIntervalSinceReferenceDate)
                MeshGradient(width: 3, height: 3, points: [
                    [0, 0], [0.5, 0], [1, 0],
                    [0, 0.5],
                    [0.5 + 0.18 * sin(t * 0.45), 0.5 + 0.15 * cos(t * 0.37)],
                    [1, 0.5],
                    [0, 1],
                    [0.5 + 0.12 * cos(t * 0.55), 1],
                    [1, 1]
                ], colors: [
                    Color(red: 0.49, green: 0.18, blue: 0.58),
                    Color(red: 0.15, green: 0.39, blue: 0.92),
                    Color(red: 0.58, green: 0.20, blue: 0.92),
                    Color(red: 0.31, green: 0.11, blue: 0.59),
                    Color(red: 0.49, green: 0.23, blue: 0.93),
                    Color(red: 0.11, green: 0.31, blue: 0.85),
                    Color(red: 0.43, green: 0.16, blue: 0.85),
                    Color(red: 0.22, green: 0.19, blue: 0.64),
                    Color(red: 0.12, green: 0.11, blue: 0.29)
                ])
                .blur(radius: 6)
            }
        } else {
            LinearGradient(
                colors: [
                    Color(red: 0.49, green: 0.18, blue: 0.58),
                    Color(red: 0.15, green: 0.39, blue: 0.92),
                    Color(red: 0.43, green: 0.16, blue: 0.85),
                    Color(red: 0.12, green: 0.11, blue: 0.29)
                ],
                startPoint: .topLeading,
                endPoint: .bottomTrailing
            )
        }
    }
}
